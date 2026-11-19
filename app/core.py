"""结构版本、批次上传、计算列与统一查询的核心逻辑。

设计要点：
- 原始列有稳定标识 column_id（station / hour_utc / c1 / c2 …）。
  改名、扩型都不改变 column_id，因此跨多版连续改名仍能按它找回旧值。
- 观测值按上传时的解析结果原样存入 values_json（{column_id: value}），
  之后任何版本操作都不改写它：旧批次保留原值与原结构；
  新增列在旧行自然为空；整数扩小数不回填、不改数。
- 计算列有独立稳定标识 computed_id（d1 / d2 …），名称不可变；公式只按
  column_id / computed_id 取数，原始列改名、扩型不影响公式取数。- 口径（定义）不可变：发布新定义只追加 def_no，同时生成新的结构版本，
  schema_version_computed 记录“该版本每个计算列用哪一版口径”。
- 间接升版：某计算列发布新定义时，直接或间接引用它的计算列会自动复制
  一份新口径（公式不变、绑定到依赖的新口径）；无关计算列沿用原口径。
- 发布时若引用无效（原始列不在本版 / 计算列不存在）或成环，整体拒绝，
  不写入任何版本、口径或快照。
- 查询按所选“口径版本”的快照求值：复算旧口径时，整条依赖链都使用
  当时的口径；快照与观测在同一事务中读取，永远只看到完整的已提交口径。
"""
from __future__ import annotations

import csv
import io
import json
import math
import re
from datetime import datetime, timezone

DATA_TYPES = ("integer", "float", "text")
SYSTEM_TYPES = ("station", "hour")
COMPUTE_OPS = ("+", "-", "*", "/")
KIND_LABELS = {"const": "常量", "column": "原始列", "computed": "计算列"}
MAX_ERRORS = 20
MAX_LIMIT = 10000


class DomainError(Exception):
    """业务规则冲突（映射为 HTTP 400）。"""

    def __init__(self, message: str, errors: list[str] | None = None):
        super().__init__(message)
        self.message = message
        self.errors = errors or []


class NotFoundError(DomainError):
    """资源不存在（映射为 HTTP 404）。"""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ================================================================ 原始列读取

def _col_dict(r) -> dict:
    return {
        "column_id": r["column_id"],
        "name": r["name"],
        "type": r["type"],
        "position": r["position"],
        "nullable": bool(r["nullable"]),
    }


def get_active_version(conn) -> dict | None:
    r = conn.execute(
        "SELECT * FROM schema_versions WHERE status='active' "
        "ORDER BY version_no DESC LIMIT 1"
    ).fetchone()
    return dict(r) if r else None


def get_version(conn, version_no: int) -> dict | None:
    r = conn.execute(
        "SELECT * FROM schema_versions WHERE version_no=?", (version_no,)
    ).fetchone()
    return dict(r) if r else None


def version_columns(conn, version_id: int) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM schema_columns WHERE version_id=? ORDER BY position",
        (version_id,),
    ).fetchall()
    return [_col_dict(r) for r in rows]


def list_batches(conn) -> list[dict]:
    rows = conn.execute(
        """
        SELECT b.id, b.filename, b.row_count, b.uploaded_at, v.version_no
        FROM batches b JOIN schema_versions v ON v.id = b.version_id
        ORDER BY b.id
        """
    ).fetchall()
    return [dict(r) for r in rows]


# ================================================================ 口径读取

def _def_dict(r) -> dict:
    return {
        "def_id": r["def_id"],
        "computed_id": r["computed_id"],
        "def_no": r["def_no"],
        "op": r["op"],
        "left": {
            "kind": r["left_kind"], "ref": r["left_ref"],
            "value": json.loads(r["left_value_json"]),
        },
        "right": {
            "kind": r["right_kind"], "ref": r["right_ref"],
            "value": json.loads(r["right_value_json"]),
        },
        "created_version_no": r["created_version_no"],
        "created_at": r["created_at"],
    }


def _snapshot(conn, version_id: int) -> dict[str, dict]:
    """某结构版本的口径快照：computed_id -> 定义（含计算列名）。

    这是“当时的口径”的唯一事实来源；旧口径复算、依赖链求值都用它。
    """
    rows = conn.execute(
        """
        SELECT c.name AS computed_name, d.*
        FROM schema_version_computed s
        JOIN computed_columns c ON c.computed_id = s.computed_id
        JOIN computed_definitions d ON d.def_id = s.def_id
        WHERE s.version_id=?
        ORDER BY c.name
        """,
        (version_id,),
    ).fetchall()
    return {
        r["computed_id"]: {**_def_dict(r), "name": r["computed_name"]}
        for r in rows
    }


def _computed_names(conn) -> dict[str, str]:
    return {r["computed_id"]: r["name"]
            for r in conn.execute("SELECT computed_id, name FROM computed_columns")}


def _col_names_at(conn, version_no: int) -> dict[str, str]:
    """某版本发布时的原始列名，用于按当时口径展示公式。"""
    rows = conn.execute(
        "SELECT c.column_id, c.name FROM schema_columns c "
        "JOIN schema_versions v ON v.id = c.version_id WHERE v.version_no=?",
        (version_no,),
    ).fetchall()
    return {r["column_id"]: r["name"] for r in rows}


def _formula(d: dict, col_names: dict[str, str], comp_names: dict[str, str]) -> str:
    def side(o):
        if o["kind"] == "const":
            return str(o["value"])
        if o["kind"] == "column":
            return col_names.get(o["ref"], o["ref"])
        return comp_names.get(o["ref"], o["ref"])

    return f"{side(d['left'])} {d['op']} {side(d['right'])}"


def _formula_at(conn, d: dict, comp_names: dict[str, str]) -> str:
    """按该口径发布时的列名展示公式（历史口径不被后续改名改写）。"""
    names = _col_names_at(conn, d["created_version_no"])
    return _formula(d, names, comp_names)
def version_bundle(conn, v: dict, snap: dict | None = None) -> dict:
    cols = version_columns(conn, v["id"])
    snap = snap if snap is not None else _snapshot(conn, v["id"])
    col_names = {c["column_id"]: c["name"] for c in cols}
    comp_names = _computed_names(conn)
    computed = []
    for d in snap.values():
        # 公式用该口径发布时的列名（与创建版本一致）
        computed.append({
            "computed_id": d["computed_id"],
            "name": d["name"],
            "def_id": d["def_id"],
            "def_no": d["def_no"],
            "op": d["op"],
            "left": d["left"],
            "right": d["right"],
            "formula": _formula_at(conn, d, comp_names),
            "created_version_no": d["created_version_no"],
        })
    return {**v, "columns": cols, "computed_columns": computed}


def list_versions(conn) -> list[dict]:
    rows = conn.execute("SELECT * FROM schema_versions ORDER BY version_no").fetchall()
    return [version_bundle(conn, dict(r)) for r in rows]


def list_computed_columns(conn) -> list[dict]:
    """计算列及其最新口径、全部历史口径。"""
    comp_names = _computed_names(conn)
    out = []
    for c in conn.execute(
        "SELECT * FROM computed_columns ORDER BY computed_id"
    ).fetchall():
        history = [_def_dict(r) for r in conn.execute(
            "SELECT * FROM computed_definitions WHERE computed_id=? ORDER BY def_no",
            (c["computed_id"],),
        ).fetchall()]
        for h in history:
            # 用该口径发布时的列名，避免历史口径被现役改名“改写”
            names = _col_names_at(conn, h["created_version_no"])
            h["formula"] = _formula(h, names, comp_names)
        out.append({
            "computed_id": c["computed_id"],
            "name": c["name"],
            "created_version_no": c["created_version_no"],
            "created_at": c["created_at"],
            "current_def": history[-1] if history else None,
            "formula": history[-1]["formula"] if history else "",
            "definitions": history,
        })
    return out


# ================================================================ 原始列发布

def _fresh_column_ids(conn, n: int) -> list[str]:
    maxn = 0
    for (cid,) in conn.execute("SELECT column_id FROM schema_columns"):
        if cid.startswith("c") and cid[1:].isdigit():
            maxn = max(maxn, int(cid[1:]))
    return [f"c{maxn + i + 1}" for i in range(n)]


def _fresh_computed_id(conn) -> str:
    maxn = 0
    for (cid,) in conn.execute("SELECT computed_id FROM computed_columns"):
        if cid.startswith("d") and cid[1:].isdigit():
            maxn = max(maxn, int(cid[1:]))
    return f"d{maxn + 1}"


def _check_name(raw, idx: int, errors: list[str]) -> str:
    name = raw.strip() if isinstance(raw, str) else ""
    if not name:
        errors.append(f"第 {idx} 列：列名不能为空")
    elif len(name) > 100:
        errors.append(f"第 {idx} 列：列名过长（超过 100 字符）")
    return name


def _check_dup_names(names: list[str], errors: list[str]) -> None:
    seen, dup = set(), set()
    for n in names:
        if n and n in seen:
            dup.add(n)
        seen.add(n)
    if dup:
        errors.append("列名重复：" + "、".join(sorted(dup)))


def _build_initial(conn, cols_spec) -> list[dict]:
    errors: list[str] = []
    items = []  # [role, name, type]
    n_station = n_hour = 0
    for i, c in enumerate(cols_spec, 1):
        if not isinstance(c, dict):
            errors.append(f"第 {i} 列：必须是对象")
            continue
        role = c.get("role") or "data"
        if role not in ("station", "hour", "data"):
            errors.append(f"第 {i} 列：role 只能是 station / hour / data")
            continue
        name = _check_name(c.get("name"), i, errors)
        if role == "station":
            n_station += 1
            items.append([role, name, "station"])
        elif role == "hour":
            n_hour += 1
            items.append([role, name, "hour"])
        else:
            ctype = c.get("type")
            if ctype not in DATA_TYPES:
                errors.append(
                    f"第 {i} 列（{name or '?'}）：数据列类型必须是 integer / float / text"
                )
                continue
            items.append([role, name, ctype])
    if n_station != 1:
        errors.append("初版结构必须且只能包含 1 个站号列（role=station）")
    if n_hour != 1:
        errors.append("初版结构必须且只能包含 1 个 UTC 小时列（role=hour）")
    _check_dup_names([n for _, n, _ in items], errors)
    if errors:
        raise DomainError("初版结构校验失败", errors)
    fresh = iter(_fresh_column_ids(conn, sum(1 for r, _, _ in items if r == "data")))
    out = []
    for pos, (role, name, ctype) in enumerate(items):
        cid = "station" if role == "station" else "hour_utc" if role == "hour" else next(fresh)
        out.append({"column_id": cid, "name": name, "type": ctype,
                    "position": pos, "nullable": True})
    return out


def _build_evolution(conn, old_cols, cols_spec) -> list[dict]:
    errors: list[str] = []
    old_by_id = {c["column_id"]: c for c in old_cols}
    used: dict[str, str] = {}
    entries = []  # {column_id|None, name, type}
    for i, c in enumerate(cols_spec, 1):
        if not isinstance(c, dict):
            errors.append(f"第 {i} 列：必须是对象")
            continue
        name = _check_name(c.get("name"), i, errors)
        src = c.get("source")
        if isinstance(src, (list, tuple, dict)):
            errors.append(
                f"第 {i} 列（{name or '?'}）：不支持两列合并（source 只能是单个来源列标识）"
            )
            continue
        if src is None:
            ctype = c.get("type")
            if ctype not in DATA_TYPES:
                errors.append(f"第 {i} 列（{name or '?'}）：新增列类型必须是 integer / float / text")
                continue
            if c.get("nullable") is False:
                errors.append(
                    f"第 {i} 列（{name or '?'}）：新增列必须可空（nullable=true），否则历史批次无法补值"
                )
                continue
            entries.append({"column_id": None, "name": name, "type": ctype})
            continue
        if src in used:
            errors.append(
                f"第 {i} 列（{name or '?'}）：来源列 {src} 已映射到「{used[src]}」，不允许一列拆成多列"
            )
            continue
        old = old_by_id.get(src)
        if old is None:
            errors.append(f"第 {i} 列（{name or '?'}）：来源列 {src} 在现役结构中不存在")
            continue
        used[src] = name
        new_type = old["type"]
        req = c.get("type")
        if req is not None and req != old["type"]:
            if old["type"] in SYSTEM_TYPES:
                errors.append(f"第 {i} 列（{name or '?'}）：站号 / 小时列的类型不可更改")
            elif old["type"] == "integer" and req == "float":
                new_type = "float"  # 唯一允许的扩型：整数 → 小数
            else:
                errors.append(
                    f"第 {i} 列（{name or '?'}）：只允许 整数→小数 扩型，不允许 {old['type']}→{req}"
                )
        entries.append({"column_id": src, "name": name, "type": new_type})
    missing = [c for c in old_cols if c["column_id"] not in used]
    if missing:
        errors.append(
            "不允许删除旧列，以下现役列缺少映射："
            + "、".join(f"{c['name']}({c['column_id']})" for c in missing)
        )
    _check_dup_names([e["name"] for e in entries], errors)
    if errors:
        raise DomainError("新版本结构校验失败", errors)
    fresh = iter(_fresh_column_ids(conn, sum(1 for e in entries if e["column_id"] is None)))
    out = []
    for pos, e in enumerate(entries):
        cid = e["column_id"] or next(fresh)
        out.append({"column_id": cid, "name": e["name"], "type": e["type"],
                    "position": pos, "nullable": True})
    return out


# ================================================================ 计算列发布

def _parse_const(raw, where: str, errors: list[str]):
    """常量只接受有限数值；文本列不参与四则运算，因此不允许文本常量。"""
    if isinstance(raw, bool) or raw is None:
        errors.append(f"{where}：常量必须是数值")
        return None
    if isinstance(raw, (int, float)):
        v = raw
    elif isinstance(raw, str):
        s = raw.strip()
        if _INT_RE.match(s):
            v = int(s)
        else:
            try:
                v = float(s)
            except ValueError:
                errors.append(f"{where}：常量 {raw!r} 不是数值")
                return None
    else:
        errors.append(f"{where}：常量必须是数值")
        return None
    if not math.isfinite(v):
        errors.append(f"{where}：常量必须是有限数值")
        return None
    return v


def _parse_operand(raw, where: str, raw_types: dict[str, str],
                   raw_name_map: dict[str, str],
                   computed_alias: dict[str, str],
                   errors: list[str]):
    """解析操作数。column 的 ref 可以是稳定标识或本版列名（发布时解析为标识）；
    computed 的 ref 可以是稳定标识、现役名称、本次新列的 key 或名称。"""
    if not isinstance(raw, dict):
        errors.append(f"{where}：操作数必须是对象 {{kind, ref/value}}")
        return None
    kind = raw.get("kind")
    if kind == "const":
        v = _parse_const(raw.get("value"), where, errors)
        if v is None:
            return None
        return {"kind": "const", "ref": None, "value": v}
    if kind == "column":
        ref = raw.get("ref")
        if not isinstance(ref, str) or not ref:
            errors.append(f"{where}：原始列操作数缺少 ref（稳定列标识或列名）")
            return None
        cid = ref if ref in raw_types else raw_name_map.get(ref)
        if cid is None:
            errors.append(f"{where}：原始列 {ref!r} 不在本次发布的版本中")
            return None
        if raw_types[cid] in SYSTEM_TYPES:
            errors.append(f"{where}：站号 / 小时列（{ref!r}）不能参与四则运算")
            return None
        if raw_types[cid] == "text":
            errors.append(f"{where}：文本列 {ref!r} 不能参与四则运算")
            return None
        return {"kind": "column", "ref": cid, "value": None}
    if kind == "computed":
        ref = raw.get("ref")
        if not isinstance(ref, str) or not ref:
            errors.append(f"{where}：计算列操作数缺少 ref（计算列标识 / 名称 / 新建 key）")
            return None
        if ref not in computed_alias:
            errors.append(
                f"{where}：计算列 {ref!r} 不存在（新建列请引用其名称或 key）"
            )
            return None
        return {"kind": "computed", "ref": computed_alias[ref], "value": None}
    errors.append(f"{where}：kind 只能是 const / column / computed")
    return None


def _deps_of_def(d: dict) -> set[str]:
    return {o["ref"] for o in (d["left"], d["right"])
            if o["kind"] == "computed" and o["ref"]}


def _cycle_check(graph: dict[str, set[str]]) -> list[str]:
    """graph: 节点 -> 它依赖的节点。返回成环的节点（空表示无环）。"""
    indeg = {n: len(deps) for n, deps in graph.items()}
    children: dict[str, list[str]] = {n: [] for n in graph}
    for n, deps in graph.items():
        for d in deps:
            children[d].append(n)
    ready = [n for n, k in indeg.items() if k == 0]
    done = 0
    while ready:
        n = ready.pop()
        done += 1
        for ch in children[n]:
            indeg[ch] -= 1
            if indeg[ch] == 0:
                ready.append(ch)
    if done == len(graph):
        return []
    return sorted(n for n, k in indeg.items() if k > 0)


def _dependent_set(old_defs: dict[str, dict], changed: set[str]) -> set[str]:
    """在现役口径依赖图上，找出直接或间接引用 changed 的计算列。"""
    children: dict[str, set[str]] = {cid: set() for cid in old_defs}
    for cid, d in old_defs.items():
        for dep in _deps_of_def(d):
            if dep in children:
                children[dep].add(cid)
    seen: set[str] = set()
    stack = list(changed)
    while stack:
        n = stack.pop()
        for ch in children.get(n, ()):
            if ch not in seen:
                seen.add(ch)
                stack.append(ch)
    return seen


def _resolve_computed(conn, spec, old_svc: dict[str, int],
                      raw_cols: list[dict], errors: list[str]) -> list[dict]:
    """校验计算列规格并计算本版本应采用的口径。

    规格项（按出现顺序）：
      {name, op, left, right, key?}        新计算列；key 供同批其他新列引用
      {column_id, op, left, right}         为现役计算列发布新口径
    操作数 kind=computed 时 ref 可以是现役 computed_id / 名称，或新建列的 key / 名称。

    返回 entries，每项：
      computed_id, key, name, def_id(None=新定义), is_new_definition,
      op, left, right, auto_bumped
    """
    raw_types = {c["column_id"]: c["type"] for c in raw_cols}
    raw_names = [c["name"] for c in raw_cols]
    raw_name_map = {c["name"]: c["column_id"] for c in raw_cols}
    old_defs = {}
    for cid, def_id in old_svc.items():
        r = conn.execute("SELECT * FROM computed_definitions WHERE def_id=?",
                         (def_id,)).fetchone()
        if r is not None:
            old_defs[cid] = _def_dict(r)
    old_names = _computed_names(conn)
    seen_names: set[str] = set(old_names.values())
    listed: dict[str, dict] = {}
    new_items: list[dict] = []
    seen_keys: set[str] = set()
    for i, c in enumerate(spec, 1):
        if not isinstance(c, dict):
            errors.append(f"计算列第 {i} 项：必须是对象")
            continue
        cid = c.get("column_id")
        if cid is None:
            name = _check_name(c.get("name"), i, errors)
            if not name:
                continue
            if name in seen_names:
                errors.append(f"计算列第 {i} 项：名称「{name}」与已有列或计算列重复")
                continue
            seen_names.add(name)
            key = c.get("key")
            if key is not None:
                if not isinstance(key, str) or not key:
                    errors.append(f"计算列第 {i} 项（{name}）：key 必须是非空字符串")
                    continue
                if key in old_svc or key in seen_keys:
                    errors.append(f"计算列第 {i} 项（{name}）：key {key!r} 重复或与现役标识冲突")
                    continue
                seen_keys.add(key)
            new_items.append({"index": i, "name": name, "key": key, "spec": c})
            continue
        if not isinstance(cid, str) or cid not in old_svc:
            errors.append(f"计算列第 {i} 项：column_id {cid!r} 不是现役计算列")
            continue
        if cid in listed:
            errors.append(f"计算列第 {i} 项：计算列 {cid} 重复出现")
            continue
        if c.get("op") is None and c.get("left") is None and c.get("right") is None:
            errors.append(
                f"计算列第 {i} 项（{old_names.get(cid, cid)}）：发布新口径须提供 op / left / right；"
                f"若只是沿用，请不要列出该列"
            )
            continue
        req_name = c.get("name")
        if req_name is not None and req_name != old_names.get(cid):
            errors.append(
                f"计算列第 {i} 项：计算列名称不可变更（{old_names.get(cid, cid)} → {req_name}），"
                f"跨版本沿用同一名称以保证口径可追溯"
            )
            continue
        listed[cid] = {"index": i, "spec": c}

    # 操作数别名：现役 computed_id / 现役名称 / 本次新列 key 或名称 → 内部节点
    alias: dict[str, str] = {}
    for cid, name in old_names.items():
        if cid in old_svc:
            alias[cid] = cid
            alias[name] = cid
    for item in new_items:
        node = item["key"] or f"@new{item['index']}"
        alias[item["name"]] = node
        if item["key"]:
            alias[item["key"]] = node

    def parse_pair(i, label, c):
        op = c.get("op")
        if op not in COMPUTE_OPS:
            errors.append(f"计算列第 {i} 项（{label}）：op 只能是 + - * /")
            return None
        left = _parse_operand(c.get("left"), f"计算列第 {i} 项（{label}）左操作数",
                              raw_types, raw_name_map, alias, errors)
        right = _parse_operand(c.get("right"), f"计算列第 {i} 项（{label}）右操作数",
                               raw_types, raw_name_map, alias, errors)
        if left is None or right is None:
            return None
        return {"op": op, "left": left, "right": right}

    parsed: dict[str, dict] = {}
    for cid, item in listed.items():
        p = parse_pair(item["index"], old_names.get(cid, cid), item["spec"])
        if p is not None:
            parsed[cid] = p

    parsed_new: dict[str, dict] = {}
    for item in new_items:
        p = parse_pair(item["index"], item["name"], item["spec"])
        if p is not None:
            parsed_new[item["key"] or f"@new{item['index']}"] = {
                "name": item["name"], "key": item["key"], **p}

    if errors:
        return []

    # 间接升版：显式改口径的计算列视为变更，其直接/间接依赖者自动复制新口径。
    changed = set(parsed)
    dependents = _dependent_set(old_defs, changed)
    auto_bump = (dependents - set(parsed)) & set(old_svc)

    entries: list[dict] = []
    final_defs: dict[str, dict] = {}  # 规划节点（现役 id + 新列 key）-> 口径
    for cid, def_id in old_svc.items():
        if cid in parsed or cid in auto_bump:
            continue
        entries.append({"computed_id": cid, "key": None, "name": old_names[cid],
                        "def_id": def_id, "is_new_definition": False,
                        "auto_bumped": False})
        final_defs[cid] = old_defs[cid]
    for cid in sorted(auto_bump):
        d = old_defs[cid]
        entries.append({"computed_id": cid, "key": None, "name": old_names[cid],
                        "def_id": None, "is_new_definition": True,
                        "auto_bumped": True, "op": d["op"],
                        "left": d["left"], "right": d["right"]})
        final_defs[cid] = d
    for cid, p in parsed.items():
        entries.append({"computed_id": cid, "key": None, "name": old_names[cid],
                        "def_id": None, "is_new_definition": True,
                        "auto_bumped": False, **p})
        final_defs[cid] = p
    for node, p in parsed_new.items():
        entries.append({"computed_id": None, "key": node, "name": p["name"],
                        "def_id": None, "is_new_definition": True,
                        "auto_bumped": False, "op": p["op"],
                        "left": p["left"], "right": p["right"]})
        final_defs[node] = p

    # 悬空引用 + 成环
    graph: dict[str, set[str]] = {}
    for node, d in final_defs.items():
        deps = _deps_of_def(d)
        for dep in deps:
            if dep not in final_defs:
                errors.append(f"计算列 {node} 引用了本版本中不存在的计算列 {dep}")
        graph[node] = {x for x in deps if x in final_defs}
    if errors:
        return []
    cyclic = _cycle_check(graph)
    if cyclic:
        errors.append("计算列引用成环：" + " → ".join(cyclic + [cyclic[0]]))
        return []

    names = raw_names + [e["name"] for e in entries]
    _check_dup_names(names, errors)
    if errors:
        return []
    return entries


def _insert_definition(conn, cid: str, op, left, right, version_no: int) -> int:
    nxt = conn.execute(
        "SELECT COALESCE(MAX(def_no),0)+1 FROM computed_definitions WHERE computed_id=?",
        (cid,),
    ).fetchone()[0]
    cur = conn.execute(
        "INSERT INTO computed_definitions (computed_id, def_no, op, "
        "left_kind, left_ref, left_value_json, right_kind, right_ref, right_value_json, "
        "created_version_no, created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (cid, nxt, op,
         left["kind"], left["ref"], json.dumps(left["value"], ensure_ascii=False),
         right["kind"], right["ref"], json.dumps(right["value"], ensure_ascii=False),
         version_no, _now()),
    )
    return cur.lastrowid


# ================================================================ 发布

def publish_version(conn, spec) -> dict:
    """发布新版本。先完整校验，全部通过后才在事务内写入；失败回滚，现役版本不变。

    spec:
      note: 可选
      columns: 原始列定义（初版角色声明；演化时按稳定标识映射）
      computed_columns: 可选。计算列定义：
        - 新列：{name, op, left, right}
        - 现役列改口径：{column_id, op, left, right}
        未列出的现役计算列：无关则沿用原口径；若直接/间接引用了本次改动的
        计算列，则自动升版（公式不变）。
      操作数：{kind:"const", value:5} / {kind:"column", ref:"c1"} /
              {kind:"computed", ref:"d1"}
    """
    if not isinstance(spec, dict):
        raise DomainError("请求体必须是 JSON 对象")
    cols_spec = spec.get("columns")
    note = spec.get("note") or ""
    if not isinstance(note, str):
        raise DomainError("note 必须是字符串")
    if not isinstance(cols_spec, list) or not cols_spec:
        raise DomainError("columns 必须是非空数组")
    computed_spec = spec.get("computed_columns")
    if computed_spec is None:
        computed_spec = []
    if not isinstance(computed_spec, list):
        raise DomainError("computed_columns 必须是数组")

    active = get_active_version(conn)
    if active is None:
        new_cols = _build_initial(conn, cols_spec)
        old_svc: dict[str, int] = {}
    else:
        new_cols = _build_evolution(conn, version_columns(conn, active["id"]), cols_spec)
        old_svc = {cid: d["def_id"] for cid, d in _snapshot(conn, active["id"]).items()}

    raw_types = {c["column_id"]: c["type"] for c in new_cols}
    raw_names = [c["name"] for c in new_cols]
    errors: list[str] = []
    entries = _resolve_computed(conn, computed_spec, old_svc, new_cols, errors)
    if errors:
        raise DomainError("计算列校验失败", errors)

    _check_dup_names(raw_names + [e["name"] for e in entries], errors)
    if errors:
        raise DomainError("结构校验失败", errors)

    maxno = conn.execute("SELECT COALESCE(MAX(version_no),0) FROM schema_versions").fetchone()[0]
    version_no = maxno + 1
    if active is not None:
        conn.execute("UPDATE schema_versions SET status='superseded' WHERE id=?",
                     (active["id"],))
    cur = conn.execute(
        "INSERT INTO schema_versions (version_no, status, note, created_at) VALUES (?,?,?,?)",
        (version_no, "active", note.strip(), _now()),
    )
    vid = cur.lastrowid
    for c in new_cols:
        conn.execute(
            "INSERT INTO schema_columns (version_id, column_id, name, type, position, nullable)"
            " VALUES (?,?,?,?,?,?)",
            (vid, c["column_id"], c["name"], c["type"], c["position"], 1),
        )
    # 先为新计算列分配稳定标识，这样同批新列之间可以互相引用
    key_map: dict[str, str] = {}
    for e in entries:
        if e["computed_id"] is None:
            cid = _fresh_computed_id(conn)
            conn.execute(
                "INSERT INTO computed_columns (computed_id, name, created_version_no, created_at)"
                " VALUES (?,?,?,?)",
                (cid, e["name"], version_no, _now()),
            )
            e["computed_id"] = cid
            if e.get("key"):
                key_map[e["key"]] = cid

    def resolve_ref(o):
        if o["kind"] == "computed" and o["ref"] in key_map:
            return {**o, "ref": key_map[o["ref"]]}
        return o

    for e in entries:
        cid = e["computed_id"]
        def_id = e["def_id"]
        if e["is_new_definition"]:
            def_id = _insert_definition(
                conn, cid, e["op"], resolve_ref(e["left"]),
                resolve_ref(e["right"]), version_no)
            e["def_id"] = def_id
        conn.execute(
            "INSERT INTO schema_version_computed (version_id, computed_id, def_id)"
            " VALUES (?,?,?)",
            (vid, cid, def_id),
        )
    row = conn.execute("SELECT * FROM schema_versions WHERE id=?", (vid,)).fetchone()
    return version_bundle(conn, dict(row), _snapshot(conn, vid))


# ================================================================ 上传

_INT_RE = re.compile(r"^[+-]?\d+$")
_HOUR_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})[T ](\d{2})(?::(\d{2}))?(?::(\d{2}))?Z?$")


def normalize_hour(raw) -> str:
    """把常见 UTC 小时写法规范为 YYYY-MM-DDTHH:MM，便于按区间比较。"""
    s = (raw or "").strip()
    m = _HOUR_RE.match(s)
    if not m:
        raise ValueError(f"无法识别的小时格式 {s!r}（应为如 2026-11-18T03:00）")
    y, mo, d, h, mi, sec = m.groups()
    if sec and sec != "00":
        raise ValueError(f"小时须对齐到分钟，秒必须为 00：{s!r}")
    mi = mi or "00"
    try:
        datetime(int(y), int(mo), int(d), int(h), int(mi))
    except ValueError as e:
        raise ValueError(f"非法时间 {s!r}：{e}")
    return f"{y}-{mo}-{d}T{h}:{mi}"


def _parse_value(raw, ctype):
    """返回 (值, 错误)。空单元格 → (None, None)。数值按原始读数形态保存。"""
    s = (raw or "").strip()
    if s == "":
        return None, None
    if ctype == "integer":
        if not _INT_RE.match(s):
            return None, f"期望整数，得到 {s!r}"
        return int(s), None
    if ctype == "float":
        if _INT_RE.match(s):
            return int(s), None  # 保留原始读数形态（5 不写成 5.0）
        try:
            v = float(s)
        except ValueError:
            return None, f"期望小数，得到 {s!r}"
        if not math.isfinite(v):
            return None, f"期望有限小数，得到 {s!r}"
        return v, None
    return s, None  # text 原样保留


def upload_csv(conn, filename: str, text: str) -> dict:
    """按现役结构校验并入库。任何一行不合格则整批拒绝，不产生半成品批次。

    计算列不入 CSV、不落库：查询时按所选口径实时求值。
    """
    active = get_active_version(conn)
    if active is None:
        raise DomainError("尚未发布任何结构版本，请先发布初版结构")
    cols = version_columns(conn, active["id"])
    names = [c["name"] for c in cols]
    try:
        reader = csv.DictReader(io.StringIO(text))
        header = reader.fieldnames
    except csv.Error as e:
        raise DomainError(f"CSV 解析失败：{e}")
    if not header:
        raise DomainError("CSV 缺少表头")
    if len(set(header)) != len(header):
        raise DomainError("CSV 表头存在重复列名")
    missing = [n for n in names if n not in header]
    extra = [h for h in header if h not in names]
    if missing or extra:
        errs = []
        if missing:
            errs.append("缺少列：" + "、".join(missing))
        if extra:
            errs.append("多余列：" + "、".join(extra))
        errs.append(f"现役结构（v{active['version_no']}）要求表头：" + "、".join(names))
        raise DomainError("CSV 表头与现役结构不一致", errs)
    rows, errors = [], []
    for ln, raw in enumerate(reader, start=2):
        row_err: list[str] = []
        if None in raw:
            row_err.append(f"第 {ln} 行：列数多于表头")
        rec: dict = {}
        station = hour = None
        for c in cols:
            cell = raw.get(c["name"])
            if c["type"] == "station":
                s = (cell or "").strip()
                if not s:
                    row_err.append(f"第 {ln} 行：站号为空")
                elif len(s) > 100:
                    row_err.append(f"第 {ln} 行：站号过长（超过 100 字符）")
                else:
                    station = s
            elif c["type"] == "hour":
                try:
                    hour = normalize_hour(cell)
                except ValueError as e:
                    row_err.append(f"第 {ln} 行：{e}")
            else:
                v, err = _parse_value(cell, c["type"])
                if err:
                    row_err.append(f"第 {ln} 行 列「{c['name']}」：{err}")
                else:
                    rec[c["column_id"]] = v
        if row_err:
            errors.extend(row_err)
        else:
            rows.append((station, hour, json.dumps(rec, ensure_ascii=False)))
        if len(errors) >= MAX_ERRORS:
            errors.append("……错误过多，仅显示前部分")
            break
    if errors:
        raise DomainError(f"CSV 校验未通过（{len(errors)} 处问题），未写入任何数据", errors)
    if not rows:
        raise DomainError("CSV 没有数据行")
    cur = conn.execute(
        "INSERT INTO batches (version_id, filename, row_count, uploaded_at) VALUES (?,?,?,?)",
        (active["id"], filename, len(rows), _now()),
    )
    bid = cur.lastrowid
    conn.executemany(
        "INSERT INTO observations (batch_id, station, hour_utc, values_json) VALUES (?,?,?,?)",
        [(bid, s, h, j) for s, h, j in rows],
    )
    return {"batch_id": bid, "version_no": active["version_no"], "row_count": len(rows)}


# ================================================================ 求值

def _fmt_num(v):
    """积分为整数的浮点结果收敛为整数展示（如 10.0 → 10）；除法保留小数。"""
    if isinstance(v, float) and math.isfinite(v) and v.is_integer():
        return int(v)
    return v


def _eval_operand(o, raw, snap, cache, stack, names):
    """返回 (值, 缺值/错误原因)。原因沿依赖链向上传递。"""
    if o["kind"] == "const":
        return o["value"], None
    if o["kind"] == "column":
        if o["ref"] not in raw:
            return None, f"原始列「{names['columns'].get(o['ref'], o['ref'])}」({o['ref']}) 无读数"
        v = raw.get(o["ref"])
        if v is None:
            return None, f"原始列「{names['columns'].get(o['ref'], o['ref'])}」({o['ref']}) 无读数"
        if isinstance(v, str):
            return None, f"原始列「{names['columns'].get(o['ref'], o['ref'])}」({o['ref']}) 是文本，不能运算"
        return v, None
    # computed
    if o["ref"] not in snap:
        return None, f"口径缺少计算列 {o['ref']} 的定义"
    v, reason = _eval_cid(o["ref"], raw, snap, cache, stack, names)
    if v is None:
        label = names["computed"].get(o["ref"], o["ref"])
        return None, f"依赖计算列「{label}」：{reason}"
    return v, None


def _eval_cid(cid, raw, snap, cache, stack, names):
    if cid in cache:
        return cache[cid]
    if cid in stack:
        return None, "循环引用"
    d = snap[cid]
    stack = stack | {cid}
    lv, lr = _eval_operand(d["left"], raw, snap, cache, stack, names)
    rv, rr = _eval_operand(d["right"], raw, snap, cache, stack, names)
    if lv is None or rv is None:
        parts = [r for r in (lr, rr) if r]
        res = (None, "缺值：" + "；".join(parts))
        cache[cid] = res
        return res
    try:
        if d["op"] == "+":
            v = lv + rv
        elif d["op"] == "-":
            v = lv - rv
        elif d["op"] == "*":
            v = lv * rv
        else:
            if rv == 0:
                res = (None, "除零：除数为 0")
                cache[cid] = res
                return res
            v = lv / rv
    except (TypeError, OverflowError, ZeroDivisionError):
        res = (None, "运算失败：操作数类型不匹配")
        cache[cid] = res
        return res
    if isinstance(v, float) and not math.isfinite(v):
        res = (None, "运算结果溢出（非有限数值）")
        cache[cid] = res
        return res
    res = (_fmt_num(v), None)
    cache[cid] = res
    return res


def _closure(cid: str, snap: dict) -> list[str]:
    """依赖在前、自身在后；发布已保证无环。"""
    order, seen = [], set()

    def visit(x):
        if x in seen:
            return
        seen.add(x)
        d = snap[x]
        for o in (d["left"], d["right"]):
            if o["kind"] == "computed" and o["ref"] in snap:
                visit(o["ref"])
        order.append(x)

    visit(cid)
    return order


def _raw_values(row) -> dict:
    raw = json.loads(row["values_json"])
    raw["station"] = row["station"]
    raw["hour_utc"] = row["hour_utc"]
    return raw


# ================================================================ 查询

def query_observations(conn, station=None, hour_from=None, hour_to=None,
                       version_no=None, computed_ids=None, def_version_no=None,
                       limit=1000) -> dict:
    """按站号 / 时间查询原始读数与计算列。

    - version_no：批次视角。缺省=现役视角，展示全部批次并用现役列名；
      指定历史版本则只看该版批次、用当时列名。
    - def_version_no：计算列口径版本。缺省跟随批次视角（现役视角→现役口径）。
      设成旧版本即可用“当时的整条依赖链”复算同一批读数。
    - computed_ids：选择要展示的计算列；缺省为该口径下的全部计算列。
    """
    if version_no is not None:
        ver = get_version(conn, version_no)
        if ver is None:
            raise NotFoundError(f"版本 v{version_no} 不存在")
    else:
        ver = get_active_version(conn)
        if ver is None:
            raise NotFoundError("尚未发布任何结构版本")
    cols = version_columns(conn, ver["id"])

    if def_version_no is not None:
        def_ver = get_version(conn, def_version_no)
        if def_ver is None:
            raise NotFoundError(f"口径版本 v{def_version_no} 不存在")
    else:
        def_ver = ver
    snap = _snapshot(conn, def_ver["id"])

    if computed_ids:
        wanted = [x.strip() for x in computed_ids if x and x.strip()]
        unknown = [x for x in wanted if x not in snap]
        if unknown:
            raise DomainError(
                "以下计算列在口径版本 v%d 中不存在：%s"
                % (def_ver["version_no"], "、".join(unknown))
            )
    else:
        wanted = list(snap.keys())
    want_set = set(wanted)

    where, params = [], []
    if version_no is not None:
        where.append("b.version_id = ?")
        params.append(ver["id"])
    if station:
        where.append("o.station = ?")
        params.append(station.strip())
    if hour_from:
        where.append("o.hour_utc >= ?")
        params.append(hour_from)
    if hour_to:
        where.append("o.hour_utc <= ?")
        params.append(hour_to)
    sql = (
        "SELECT o.station, o.hour_utc, o.values_json, o.batch_id, "
        "b.version_id, b.filename "
        "FROM observations o JOIN batches b ON b.id = o.batch_id"
    )
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY o.station, o.hour_utc, o.batch_id, o.id LIMIT ?"
    params.append(limit + 1)
    fetched = conn.execute(sql, params).fetchall()
    truncated = len(fetched) > limit
    fetched = fetched[:limit]
    ver_map = {r["id"]: r["version_no"]
               for r in conn.execute("SELECT id, version_no FROM schema_versions")}

    col_names = {c["column_id"]: c["name"] for c in cols}
    comp_names = _computed_names(conn)
    names = {"columns": col_names, "computed": comp_names}

    # 公式文本与来源列只需按口径算一次（避免逐行查库）
    name_cache: dict[int, dict[str, str]] = {}

    def formulas_at(version_no_: int) -> dict[str, str]:
        if version_no_ not in name_cache:
            name_cache[version_no_] = _col_names_at(conn, version_no_)
        return name_cache[version_no_]

    formula_by_cid = {
        cid: _formula(d, formulas_at(d["created_version_no"]), comp_names)
        for cid, d in snap.items()
    }

    # 只对选中的计算列及其依赖求值（依赖即使未选中也需算出）
    needed: list[str] = []
    closures = {cid: _closure(cid, snap) for cid in wanted}
    for cid in wanted:
        for x in closures[cid]:
            if x not in needed:
                needed.append(x)

    rows = []
    for r in fetched:
        raw = _raw_values(r)
        cache: dict[str, tuple] = {}
        chain_values = {}
        for cid in needed:
            chain_values[cid] = _eval_cid(cid, raw, snap, cache, set(), names)

        computed = {}
        for cid in wanted:
            d = snap[cid]
            value, reason = chain_values[cid]
            chain = []
            for x in closures[cid]:
                xd = snap[x]
                xv, xr = chain_values[x]
                chain.append({
                    "computed_id": x,
                    "name": xd["name"],
                    "def_no": xd["def_no"],
                    "def_version_no": xd["created_version_no"],
                    "formula": formula_by_cid[x],
                    "value": xv,
                    "reason": xr,
                })
            sources = []
            for x in closures[cid]:
                for o in (snap[x]["left"], snap[x]["right"]):
                    if o["kind"] == "column" and o["ref"] not in sources:
                        sources.append(o["ref"])
            computed[cid] = {
                "computed_id": cid,
                "name": d["name"],
                "value": value,
                "reason": reason,
                "formula": formula_by_cid[cid],
                "def_no": d["def_no"],
                "def_version_no": d["created_version_no"],
                "def_id": d["def_id"],
                "chain": chain,
                "source_columns": sources,
            }
        values = {}
        for c in cols:
            if c["type"] == "station":
                values[c["name"]] = r["station"]
            elif c["type"] == "hour":
                values[c["name"]] = r["hour_utc"]
            else:
                values[c["name"]] = raw.get(c["column_id"])
        rows.append({
            "batch_id": r["batch_id"],
            "batch_filename": r["filename"],
            "batch_version_no": ver_map.get(r["version_id"]),
            "station": r["station"],
            "hour_utc": r["hour_utc"],
            "values": values,
            "computed": computed,
        })

    computed_meta = []
    for cid in wanted:
        d = snap[cid]
        computed_meta.append({
            "computed_id": cid,
            "name": d["name"],
            "formula": formula_by_cid[cid],
            "def_no": d["def_no"],
            "def_version_no": d["created_version_no"],
        })
    return {
        "version": version_bundle(conn, ver),
        "def_version": {"version_no": def_ver["version_no"], "id": def_ver["id"]},
        "computed_columns": computed_meta,
        "row_count": len(rows),
        "truncated": truncated,
        "rows": rows,
    }
