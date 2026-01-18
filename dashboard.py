import os
import glob
from typing import Dict, List, Tuple, Optional

import pandas as pd
import streamlit as st
import plotly.express as px


# 페이지 설정
st.set_page_config(
    page_title="CATHERO 점수 계산 대시보드", page_icon="⚔️", layout="wide"
)

st.title("⚔️ CATHERO 길드 점수 계산 대시보드")
st.caption(
    "CSV에서 길드원별 격전지 점수, 1초당 점수, 추가 점수, 최대 획득 점수를 추정·계산합니다."
)


# 경로
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")


def parse_filename(file_name: str) -> Dict[str, str]:
    """파일명 규칙: <guild>_<date>_<boss_order>_<boss_level>.csv 또는 <guild>_<date>_normal.csv
    normal 파일은 일반 몬스터 데이터를 의미하며 날짜를 포함하고 order='normal'로 처리합니다.
    """
    name = os.path.splitext(os.path.basename(file_name))[0]
    parts = name.split("_")
    if len(parts) == 3 and parts[-1].lower() == "normal":
        guild, date, _ = parts
        return {"guild": guild, "date": date, "order": "normal", "level": ""}
    if len(parts) < 4:
        # 예상치 못한 형식은 최대한 복구: 길드만 채우고 나머지는 공란
        return {
            "guild": parts[0],
            "date": parts[1] if len(parts) > 1 else "",
            "order": parts[2] if len(parts) > 2 else "",
            "level": "",
        }
    guild, date, order, level = parts[:4]
    return {"guild": guild, "date": date, "order": order, "level": level}


@st.cache_data(show_spinner=False)
def load_common_for_guild(data_dir: str, guild: str) -> pd.DataFrame:
    """길드별 공통 파일(<guild>_common.csv)을 로드하고 컬럼을 정규화합니다.
    반환 컬럼: nickname, confirmed_bonus, confirmed_extra
    """
    path = os.path.join(data_dir, f"{guild}_common.csv")
    if not os.path.exists(path):
        return pd.DataFrame(columns=["nickname", "confirmed_bonus", "confirmed_extra"])
    try:
        df = pd.read_csv(path)
        if df.empty:
            return pd.DataFrame(
                columns=["nickname", "confirmed_bonus", "confirmed_extra"]
            )
        cols = {c.lower(): c for c in df.columns}

        def pick(*cands):
            for k in cands:
                if k in cols:
                    return cols[k]
            return None

        nick_col = pick("nickname", "닉네임")
        bonus_col = pick("confirmed_bonus", "bonus", "추가점수", "확정_추가점수")
        extra_col = pick(
            "confirmed_extra",
            "extra",
            "extra_seconds",
            "추가초",
            "추가 획득 초",
            "확정_추가초",
        )
        battle_col = pick(
            "confirmed_battle", "battle", "격전지", "격전지점수", "확정_격전지"
        )
        if not nick_col:
            return pd.DataFrame(
                columns=[
                    "nickname",
                    "confirmed_bonus",
                    "confirmed_extra",
                    "confirmed_battle",
                ]
            )
        rename_map = {nick_col: "nickname"}
        if bonus_col:
            rename_map[bonus_col] = "confirmed_bonus"
        if extra_col:
            rename_map[extra_col] = "confirmed_extra"
        if battle_col:
            rename_map[battle_col] = "confirmed_battle"
        df = df.rename(columns=rename_map)
        keep = [
            c
            for c in [
                "nickname",
                "confirmed_bonus",
                "confirmed_extra",
                "confirmed_battle",
            ]
            if c in df.columns
        ]
        df = df[keep]
        df["nickname"] = df["nickname"].astype(str).str.strip()
        if "confirmed_bonus" in df.columns:
            df["confirmed_bonus"] = pd.to_numeric(
                df["confirmed_bonus"], errors="coerce"
            )
        else:
            df["confirmed_bonus"] = pd.NA
        if "confirmed_extra" in df.columns:
            df["confirmed_extra"] = pd.to_numeric(
                df["confirmed_extra"], errors="coerce"
            )
        else:
            df["confirmed_extra"] = pd.NA
        if "confirmed_battle" in df.columns:
            df["confirmed_battle"] = pd.to_numeric(
                df["confirmed_battle"], errors="coerce"
            )
        return df
    except Exception as e:
        st.warning(f"공통 파일 로드 실패: {path} ({e})")
        return pd.DataFrame(
            columns=[
                "nickname",
                "confirmed_bonus",
                "confirmed_extra",
                "confirmed_battle",
            ]
        )


@st.cache_data(show_spinner=False)
def load_all_csv(data_dir: str) -> pd.DataFrame:
    files = sorted(glob.glob(os.path.join(data_dir, "*.csv")))
    frames: List[pd.DataFrame] = []
    for f in files:
        base_name = os.path.basename(f)
        # 공통 파일은 별도 로더로 처리 (점수 집계 제외)
        if base_name.endswith("_common.csv"):
            continue
        try:
            meta = parse_filename(f)
            df = pd.read_csv(f)
            df["guild"] = meta["guild"]
            df["date"] = meta["date"]
            df["boss_order"] = meta["order"]
            df["boss_level"] = meta["level"]
            df = df.rename(
                columns={"순위": "rank", "닉네임": "nickname", "점수": "score"}
            )
            if "rank" not in df.columns:
                df["rank"] = pd.NA
            frames.append(
                df[
                    [
                        "rank",
                        "nickname",
                        "score",
                        "guild",
                        "date",
                        "boss_order",
                        "boss_level",
                    ]
                ]
            )
        except Exception as e:
            st.warning(f"파일 로드 실패: {f} ({e})")
    if not frames:
        return pd.DataFrame(
            columns=[
                "rank",
                "nickname",
                "score",
                "guild",
                "date",
                "boss_order",
                "boss_level",
            ]
        )
    out = pd.concat(frames, ignore_index=True)
    out["nickname"] = out["nickname"].astype(str).str.strip()
    out["score"] = pd.to_numeric(out["score"], errors="coerce").fillna(0).astype(int)
    return out


def infer_pps_range(
    scores: List[int],
    seconds_min: int,
    seconds_max: int,
    battle_min: int,
    battle_max: int,
    bonus_candidates: List[int],
) -> Tuple[Optional[int], Optional[int]]:
    """가능한 1초당 점수(pps) 범위 추정.
    pps = 1000 + battle*10, (score - bonus)/pps 가 초 범위에 들면 허용.
    0초(점수==보너스)도 허용하고 0.001초 단위 오차를 허용합니다.
    """
    valid: List[int] = []
    for battle in range(battle_min, battle_max + 1):
        pps = 1000 + battle * 10
        ok = True
        for s in scores:
            found = False
            for b in bonus_candidates:
                # 0초(보너스만 기록) 허용
                if s == b:
                    found = True
                    break
                secs = (s - b) / pps
                # 0.001초 단위 오차 허용
                if secs >= seconds_min - 1e-3 and secs <= seconds_max + 1e-3:
                    found = True
                    break
            if not found:
                ok = False
                break
        if ok:
            valid.append(pps)
    if not valid:
        return None, None
    return min(valid), max(valid)


def best_bonus_and_seconds(
    score: int, pps: int, bonus_candidates: List[int]
) -> Tuple[int, float]:
    """보너스 후보 중 초 값이 가장 자연스러운(소수부 작은) 조합 선택.
    0초(점수==보너스)도 허용합니다.
    """
    # 0초 특별 처리: 점수가 보너스와 동일하면 해당 보너스 확정
    for b in bonus_candidates:
        if score == b:
            return b, 0.0
        secs = (score - b) / pps
        err = abs(secs - round(secs, 3))
        if err < best_err and secs >= 0:
            best_err = err
            best_bonus = b
            best_secs = secs
    return best_bonus, round(best_secs, 3)


# 데이터 로드
if not os.path.exists(DATA_DIR):
    st.error(f"데이터 폴더가 없습니다: {DATA_DIR}")
    st.stop()

df_all = load_all_csv(DATA_DIR)
if df_all.empty:
    st.warning("CSV 데이터가 없습니다. data 폴더에 파일을 추가하세요.")
    st.stop()


# 사이드바 필터 (길드/날짜만)
st.sidebar.header("필터")
guilds = sorted(df_all["guild"].unique())
# 기본 길드는 'M'이 있으면 M, 없으면 첫 번째
default_guild_index = guilds.index("M") if "M" in guilds else 0
sel_guild = st.sidebar.selectbox("길드 선택", guilds, index=default_guild_index)

# 선택된 길드의 날짜 중 가장 최근 날짜를 기본값으로 선택
dates = sorted(df_all[df_all["guild"] == sel_guild]["date"].unique())
date_options = ["전체"] + dates
default_date_index = (len(date_options) - 1) if len(date_options) > 1 else 0
sel_date = st.sidebar.selectbox(
    "날짜 선택 (단일)", date_options, index=default_date_index
)


# 추정 상수 (고정값)
INFER_SECONDS_MIN = 0
INFER_SECONDS_MAX = 300
BATTLE_MIN_DEFAULT = 6
BATTLE_MAX_DEFAULT = 250
BONUS_CANDIDATES_DEFAULT = [0, 500, 1000, 1500, 2000, 2500, 3000]
BASE_SECONDS = 1200


# 필터 적용
if sel_date == "전체":
    filtered = df_all[df_all["guild"] == sel_guild]
else:
    filtered = df_all[(df_all["guild"] == sel_guild) & (df_all["date"] == sel_date)]
if filtered.empty:
    st.info("선택한 조건에 해당하는 데이터가 없습니다.")
    st.stop()

# 길드 공통 확정 데이터 로드
common_df = load_common_for_guild(DATA_DIR, sel_guild)
common_bonus_map: Dict[str, Optional[int]] = {}
common_extra_map: Dict[str, Optional[int]] = {}
common_battle_map: Dict[str, Optional[float]] = {}
if not common_df.empty:
    common_bonus_map = {
        r["nickname"]: (
            int(r["confirmed_bonus"]) if pd.notna(r["confirmed_bonus"]) else None
        )
        for _, r in common_df.iterrows()
    }
    common_extra_map = {
        r["nickname"]: (
            int(r["confirmed_extra"]) if pd.notna(r["confirmed_extra"]) else None
        )
        for _, r in common_df.iterrows()
    }
    if "confirmed_battle" in common_df.columns:
        common_battle_map = {
            r["nickname"]: (
                float(r["confirmed_battle"])
                if pd.notna(r["confirmed_battle"])
                else None
            )
            for _, r in common_df.iterrows()
        }


# 탭: 닉네임 추정 결과 / 계산기 / 원본 데이터(보스별)
tab1, tab2, tab3 = st.tabs(["🔎 닉네임 추정 결과", "📊 시각화", "📂 원본 데이터"])


with tab1:
    st.subheader("닉네임별 점수 추정 결과")
    rows: List[Dict[str, object]] = []
    inferred_map: Dict[str, Dict[str, Optional[int]]] = {}
    for nickname, g in filtered.groupby("nickname"):
        scores = list(g["score"].values)
        # 공통 파일의 확정 격전지 점수 우선 적용
        fixed_battle: Optional[float] = None
        if nickname in common_battle_map and common_battle_map[nickname] is not None:
            fixed_battle = float(common_battle_map[nickname])
        # 공통 파일 우선: 해당 닉네임의 확정 보너스가 있으면 고정
        fixed_bonus: Optional[int] = None
        if nickname in common_bonus_map and common_bonus_map[nickname] is not None:
            fixed_bonus = int(common_bonus_map[nickname])
        else:
            # 보너스-only 관측 → 보너스 고정(없으면 전체 후보 탐색)
            observed_bonus_vals = [
                int(x)
                for x in g["score"].astype(int)
                if int(x) in BONUS_CANDIDATES_DEFAULT
            ]
            fixed_bonus = max(observed_bonus_vals) if observed_bonus_vals else None

        # 후보 (보너스/격전지) 페어 탐색
        candidate_pairs: List[Tuple[float, int]] = []  # (battle, bonus)
        if fixed_battle is not None:
            b2 = int(round(fixed_battle * 2))
            battle = b2 / 2.0
            pps = 1000 + 5 * b2
            bonuses_to_try = (
                [fixed_bonus] if fixed_bonus is not None else BONUS_CANDIDATES_DEFAULT
            )
            for bonus in bonuses_to_try:
                ok_all = True
                for s in scores:
                    if s == bonus:
                        continue
                    diff = int(s) - int(bonus)
                    if diff < 0 or diff % pps != 0:
                        ok_all = False
                        break
                if ok_all:
                    candidate_pairs.append((battle, int(bonus)))
            # 보너스가 전혀 맞지 않더라도 표기를 위해 1개 페어 남김
            if not candidate_pairs:
                candidate_pairs.append(
                    (battle, int(fixed_bonus) if fixed_bonus is not None else 0)
                )
        else:
            for b2 in range(
                int(BATTLE_MAX_DEFAULT * 2), int(BATTLE_MIN_DEFAULT * 2) - 1, -1
            ):
                battle = b2 / 2.0
                pps = 1000 + 5 * b2  # 10*battle = 5*b2, pps는 정수
                bonuses_to_try = (
                    [fixed_bonus]
                    if fixed_bonus is not None
                    else BONUS_CANDIDATES_DEFAULT
                )
                valid_for_any_bonus = False
                for bonus in bonuses_to_try:
                    ok_all = True
                    for s in scores:
                        # 0초(보너스-only)는 항상 허용
                        if s == bonus:
                            continue
                        # 정확한 나눗셈 조건: (보스 점수 - 추가 점수) % pps == 0
                        diff = int(s) - int(bonus)
                        if diff < 0 or diff % pps != 0:
                            ok_all = False
                            break
                    if ok_all:
                        candidate_pairs.append((battle, int(bonus)))
                        valid_for_any_bonus = True
                        break
                if not valid_for_any_bonus:
                    continue

        # 페어가 없을 경우: 최소(0초 제외) 점수만으로 fallback 페어 생성
        if not candidate_pairs and not g.empty:
            g_tmp = g.copy()
            g_tmp["boss_order_num"] = pd.to_numeric(
                g_tmp["boss_order"], errors="coerce"
            )
            g_tmp = g_tmp.sort_values("boss_order_num")
            # 보스/노멀 각각의 5의 배수 중 최솟값 비교 (보너스-only 제외)
            boss_only = g_tmp[g_tmp["boss_order"].astype(str).str.lower() != "normal"]
            normal_only = g_tmp[g_tmp["boss_order"].astype(str).str.lower() == "normal"]

            def min5(df_sub: pd.DataFrame):
                x = df_sub[~df_sub["score"].isin(BONUS_CANDIDATES_DEFAULT)]
                x5 = x[x["score"].astype(int) % 5 == 0]
                if not x5.empty:
                    return x5.iloc[x5["score"].astype(int).argmin()]
                if not x.empty:
                    return x.iloc[x["score"].astype(int).argmin()]
                y5 = df_sub[df_sub["score"].astype(int) % 5 == 0]
                if not y5.empty:
                    return y5.iloc[y5["score"].astype(int).argmin()]
                return df_sub.iloc[df_sub["score"].astype(int).argmin()]

            target_boss = min5(boss_only) if not boss_only.empty else None
            target_norm = min5(normal_only) if not normal_only.empty else None

            def get_score(row):
                return int(row["score"]) if row is not None else None

            boss_min = get_score(target_boss)
            norm_min = get_score(target_norm)
            if boss_min is not None and norm_min is not None:
                target_row = target_norm if norm_min < boss_min else target_boss
            elif boss_min is not None:
                target_row = target_boss
            elif norm_min is not None:
                target_row = target_norm
            else:
                target_row = g_tmp.iloc[g_tmp["score"].astype(int).argmin()]
            score_min = int(target_row["score"])
            for b2 in range(
                int(BATTLE_MAX_DEFAULT * 2), int(BATTLE_MIN_DEFAULT * 2) - 1, -1
            ):
                battle = b2 / 2.0
                pps = 1000 + 5 * b2
                bonuses_to_try = (
                    [fixed_bonus]
                    if fixed_bonus is not None
                    else BONUS_CANDIDATES_DEFAULT
                )
                for bonus in bonuses_to_try:
                    if score_min == bonus:
                        candidate_pairs.append((battle, int(bonus)))
                        break
                    diff = int(score_min) - int(bonus)
                    if diff >= 0 and diff % pps == 0:
                        candidate_pairs.append((battle, int(bonus)))
                        break

        # 총점 대비 최대 가능 점수(10회 기준)를 만족하는 페어만 우선 필터링
        total_sum = sum(scores)
        feasible_pairs: List[Tuple[float, int]] = []
        extras_check = [0, 20, 60, 120]
        for bt, b in candidate_pairs:
            pps = int(1000 + 10 * float(bt))
            # 추가 획득 초별 최종 최대 획득 점수 중 하나라도 총점보다 커야 함
            feasible_any = any(
                (pps * (BASE_SECONDS + ex) + 10 * int(b)) > total_sum
                for ex in extras_check
            )
            if feasible_any:
                feasible_pairs.append((bt, int(b)))

        # 상위 2개만 표시(격전지 높은 순) — 만족하는 페어가 있으면 그 집합을 사용
        pairs_for_display = feasible_pairs if feasible_pairs else candidate_pairs
        pairs_for_display = sorted(pairs_for_display, key=lambda x: x[0], reverse=True)
        top_pairs = pairs_for_display[:2]
        battle_estimate_display = (
            ", ".join(
                [
                    f"{int(bt) if float(bt).is_integer() else bt}/{b}"
                    for bt, b in top_pairs
                ]
            )
            if top_pairs
            else "추정불가"
        )
        # 격전지 점수 3개 이상일 경우 리스트 표기
        battle_list_display = "-"
        if candidate_pairs:
            # 최종 최대획득점수(pps*(1200+추가초)+보너스*10)가 총점보다 큰 격전지 점수들만 리스트업
            feasible_battles = []
            for bt, b in candidate_pairs:
                pps = int(1000 + 10 * float(bt))
                if any(
                    (pps * (BASE_SECONDS + ex) + 10 * int(b)) > total_sum
                    for ex in extras_check
                ):
                    feasible_battles.append(bt)
            unique_battles = sorted(list(set(feasible_battles)), reverse=True)
            if len(unique_battles) >= 3:
                battle_list_display = ", ".join(
                    [
                        str(int(bt)) if float(bt).is_integer() else str(bt)
                        for bt in unique_battles
                    ]
                )

        # pps 및 최대 획득 점수 표시(상위 2 페어 기준으로 각각 표기)
        if candidate_pairs or fixed_battle is not None:
            # 전체 후보 pps 범위(내부 계산용)
            pps_values_all = (
                [int(1000 + 10 * float(bt)) for bt, _ in candidate_pairs]
                if candidate_pairs
                else [int(1000 + 10 * float(fixed_battle))]
            )
            pps_min = min(pps_values_all)
            pps_max = max(pps_values_all)

            # 상위 2 페어 기준 pps 표시 및 추가 초별 최대 점수 표기
            pps_values_top = (
                [int(1000 + 10 * float(bt)) for bt, _ in top_pairs]
                if top_pairs
                else [int(1000 + 10 * float(fixed_battle))]
            )
            pps_display = ", ".join(str(v) for v in pps_values_top)
            extras = [0, 20, 60, 120]
            pair_max_display = []
            if top_pairs:
                for i, (bt, b) in enumerate(top_pairs):
                    pps_i = pps_values_top[i]
                    vals_total = [
                        pps_i * (BASE_SECONDS + ex) + 10 * int(b) for ex in extras
                    ]
                    bt_str = str(int(bt)) if float(bt).is_integer() else str(bt)
                    pair_max_display.append(
                        f"{bt_str}/{int(b)}: " + ", ".join(f"{v:,}" for v in vals_total)
                    )
            else:
                # 고정 격전지만 있는 경우, 보너스는 확정 보너스(없으면 0)로 계산
                pps_i = pps_values_top[0]
                b_use = int(fixed_bonus) if fixed_bonus is not None else 0
                vals_total = [pps_i * (BASE_SECONDS + ex) + 10 * b_use for ex in extras]
                bt_str = (
                    str(int(fixed_battle))
                    if float(fixed_battle).is_integer()
                    else str(fixed_battle)
                )
                pair_max_display.append(
                    f"{bt_str}/{b_use}: " + ", ".join(f"{v:,}" for v in vals_total)
                )
            max_score_est_display = " | ".join(pair_max_display)

            # 닉네임에 대한 대표 추정(상위 1 페어) 저장: 계산기에서 사용
            if top_pairs:
                chosen_battle, chosen_bonus = top_pairs[0]
                chosen_pps = int(1000 + 10 * float(chosen_battle))
                last_bonus_display = int(chosen_bonus)
            else:
                chosen_battle = (
                    float(fixed_battle) if fixed_battle is not None else None
                )
                chosen_pps = (
                    int(1000 + 10 * float(fixed_battle))
                    if fixed_battle is not None
                    else None
                )
                last_bonus_display = int(fixed_bonus) if fixed_bonus is not None else 0
            inferred_map[nickname] = {
                "pps": chosen_pps,
                "bonus": (
                    int(last_bonus_display) if last_bonus_display is not None else None
                ),
                "battle": chosen_battle,
                "pairs": (
                    pairs_for_display
                    if top_pairs
                    else (
                        [(chosen_battle, last_bonus_display)]
                        if chosen_battle is not None
                        else []
                    )
                ),
            }
        else:
            pps_min = None
            pps_max = None
            chosen_pps = None
            pps_display = "추정불가"
            last_bonus_display = "추정불가"
            max_score_est_display = "추정불가"
            inferred_map[nickname] = {
                "pps": None,
                "bonus": None,
                "battle": None,
                "pairs": [],
            }

        # 확정 추가초/추가점수 표시용
        confirmed_bonus_disp = (
            common_bonus_map.get(nickname) if nickname in common_bonus_map else None
        )
        confirmed_extra_disp = (
            common_extra_map.get(nickname) if nickname in common_extra_map else None
        )
        # 확정 격전지 표기
        confirmed_battle_disp = (
            common_battle_map.get(nickname) if nickname in common_battle_map else None
        )
        rows.append(
            {
                "닉네임": nickname,
                "공격횟수": len(scores),
                "총점": sum(scores),
                "평균점수": int(sum(scores) / len(scores)) if scores else 0,
                "추정_격전지/추가점수": battle_estimate_display,
                "추정_1초당점수(pps)": pps_display,
                "추정_최대획득점수(0/20/60/120)": max_score_est_display,
                "추정_격전지_리스트": battle_list_display,
                "확정_격전지": (
                    f"{confirmed_battle_disp}"
                    if confirmed_battle_disp is not None
                    else "-"
                ),
                "확정_추가점수": (
                    f"{int(confirmed_bonus_disp)}"
                    if confirmed_bonus_disp is not None
                    else "-"
                ),
                "확정_추가초": (
                    f"{int(confirmed_extra_disp)}"
                    if confirmed_extra_disp is not None
                    else "-"
                ),
            }
        )

    result_df = pd.DataFrame(rows)
    # Arrow 호환을 위해 표시용 열을 문자열로 강제 변환
    for col in ["확정_격전지", "확정_추가점수", "확정_추가초"]:
        if col in result_df.columns:
            result_df[col] = result_df[col].astype(str)
    result_view = result_df.sort_values("총점", ascending=False)[
        [
            "닉네임",
            "공격횟수",
            "총점",
            "평균점수",
            "추정_격전지/추가점수",
            "추정_1초당점수(pps)",
            "추정_최대획득점수(0/20/60/120)",
            "추정_격전지_리스트",
            "확정_격전지",
            "확정_추가점수",
            "확정_추가초",
        ]
    ]
    st.dataframe(result_view, width='stretch')

    # 날짜가 전체가 아닐 때만 길드 합계 표시
    if sel_date != "전체":
        st.divider()
        st.subheader("길드 합계")
        csum1, csum2 = st.columns(2)
        with csum1:
            guild_total_score = int(filtered["score"].sum())
            st.metric("길드 총점", f"{guild_total_score:,}")
        with csum2:
            # 공통 파일의 확정 추가초만 반영, 없으면 0초로 계산
            guild_est_max_sum = 0
            for nick, info in inferred_map.items():
                pps_i = info.get("pps")
                inferred_bonus_i = info.get("bonus")
                if not pps_i or inferred_bonus_i is None:
                    continue
                # 닉네임별 확정 추가초/추가점수 반영
                ex_val = common_extra_map.get(nick)
                ex_use = int(ex_val) if ex_val is not None else 0
                b_val = common_bonus_map.get(nick)
                b_use = int(b_val) if b_val is not None else int(inferred_bonus_i)
                guild_est_max_sum += pps_i * (BASE_SECONDS + ex_use) + 10 * b_use
            st.metric(
                "길드 추정 최대획득점수(확정 추가초 기준)", f"{guild_est_max_sum:,}"
            )

    # 날짜 전체 선택 시: 닉네임별로 각 날짜의 추정 격전지/추가점수를 나란히 비교 테이블 제공
    if sel_date == "전체":
        st.divider()
        st.subheader("닉네임별 날짜 비교: 추정 격전지/추가점수")
        dates_all = sorted(filtered["date"].unique())
        nick_all = sorted(filtered["nickname"].unique())
        compare_rows: List[Dict[str, object]] = []
        for nickname in nick_all:
            row_item: Dict[str, object] = {"닉네임": nickname}
            for d in dates_all:
                g2 = filtered[
                    (filtered["nickname"] == nickname) & (filtered["date"] == d)
                ]
                if g2.empty:
                    row_item[d] = "-"
                    continue
                scores2 = list(g2["score"].values)
                # 공통 파일의 확정 격전지/보너스 적용
                observed_bonus_vals2 = [
                    int(x)
                    for x in g2["score"].astype(int)
                    if int(x) in BONUS_CANDIDATES_DEFAULT
                ]
                fixed_bonus2: Optional[int] = (
                    int(common_bonus_map[nickname])
                    if (
                        nickname in common_bonus_map
                        and common_bonus_map[nickname] is not None
                    )
                    else (max(observed_bonus_vals2) if observed_bonus_vals2 else None)
                )
                fixed_battle2: Optional[float] = (
                    float(common_battle_map[nickname])
                    if (
                        nickname in common_battle_map
                        and common_battle_map[nickname] is not None
                    )
                    else None
                )
                candidate_pairs2: List[Tuple[float, int]] = []
                if fixed_battle2 is not None:
                    b2 = int(round(fixed_battle2 * 2))
                    battle2 = b2 / 2.0
                    pps2 = 1000 + 5 * b2
                    bonuses_to_try2 = (
                        [fixed_bonus2]
                        if fixed_bonus2 is not None
                        else BONUS_CANDIDATES_DEFAULT
                    )
                    for bonus2 in bonuses_to_try2:
                        ok_all2 = True
                        for sc in scores2:
                            if sc == bonus2:
                                continue
                            diff2 = int(sc) - int(bonus2)
                            if diff2 < 0 or diff2 % pps2 != 0:
                                ok_all2 = False
                                break
                        if ok_all2:
                            candidate_pairs2.append((battle2, int(bonus2)))
                    if not candidate_pairs2:
                        candidate_pairs2.append(
                            (
                                battle2,
                                int(fixed_bonus2) if fixed_bonus2 is not None else 0,
                            )
                        )
                else:
                    for b2 in range(
                        int(BATTLE_MAX_DEFAULT * 2), int(BATTLE_MIN_DEFAULT * 2) - 1, -1
                    ):
                        battle2 = b2 / 2.0
                        pps2 = 1000 + 5 * b2
                        valid_for_any_bonus2 = False
                        bonuses_to_try2 = (
                            [fixed_bonus2]
                            if fixed_bonus2 is not None
                            else BONUS_CANDIDATES_DEFAULT
                        )
                        for bonus2 in bonuses_to_try2:
                            ok_all2 = True
                            for sc in scores2:
                                if sc == bonus2:
                                    continue
                                diff2 = int(sc) - int(bonus2)
                                if diff2 < 0 or diff2 % pps2 != 0:
                                    ok_all2 = False
                                    break
                            if ok_all2:
                                candidate_pairs2.append((battle2, int(bonus2)))
                                valid_for_any_bonus2 = True
                                break
                        if not valid_for_any_bonus2:
                            continue
                extras_check2 = [0, 20, 60, 120]
                total_sum2 = sum(scores2)
                feasible_pairs2: List[Tuple[float, int]] = []
                for bt2, bns2 in candidate_pairs2:
                    pps_tmp = int(1000 + 10 * float(bt2))
                    if any(
                        (pps_tmp * (BASE_SECONDS + ex) + 10 * int(bns2)) > total_sum2
                        for ex in extras_check2
                    ):
                        feasible_pairs2.append((bt2, int(bns2)))
                pairs_disp2 = (
                    sorted(feasible_pairs2, key=lambda x: x[0], reverse=True)
                    if feasible_pairs2
                    else sorted(candidate_pairs2, key=lambda x: x[0], reverse=True)
                )
                top2 = pairs_disp2[:2]
                battle_display2 = (
                    ", ".join(
                        [
                            f"{int(bt) if float(bt).is_integer() else bt}/{b}"
                            for bt, b in top2
                        ]
                    )
                    if top2
                    else "추정불가"
                )
                row_item[d] = battle_display2
            compare_rows.append(row_item)
        compare_df = pd.DataFrame(compare_rows)
        st.dataframe(compare_df, width='stretch')

    st.divider()
    st.subheader("🧮 최대 획득 점수 계산기")
    st.markdown(
        "기본 1200초에 추가 획득 초(0/20/60/120)를 더해 최대 점수를 계산합니다."
    )

    c1, c2, c3 = st.columns(3)
    with c1:
        manual_battle = st.number_input(
            "격전지 점수 (직접 입력)", min_value=0, max_value=500, value=100, step=1
        )
    with c2:
        bonus_per_boss = st.selectbox(
            "보스당 추가 점수", [0, 500, 1000, 1500, 2000, 2500, 3000], index=0
        )
    with c3:
        extra_seconds = st.selectbox("추가 획득 초", [0, 20, 60, 120], index=0)

    picked_pps = 1000 + manual_battle * 10
    total_secs = BASE_SECONDS + int(extra_seconds)
    total_bonus = int(bonus_per_boss)
    # 최종 최대 획득 점수 = pps*(1200+추가초) + 보너스*10
    max_score = picked_pps * total_secs + 10 * total_bonus

    # 한 줄로 결과 표기
    st.markdown(
        f"**결과**: 추정 1초당 점수(pps) {picked_pps:,} / 최대 획득 점수 {max_score:,}점"
    )

    st.divider()
    st.subheader("⏱️ 남은 공격 시간/점수 계산기 (닉네임 지정)")
    st.markdown(
        "사람을 선택하고, 현재까지의 추가 입력 점수들을 입력하여 남은 공격 시간과 남은 획득 점수를 계산합니다."
    )
    c4, c5 = st.columns(2)
    with c4:
        nickname_for_calc = st.selectbox(
            "닉네임 선택", sorted(filtered["nickname"].unique())
        )
    with c5:
        extra_seconds_assume = st.selectbox(
            "추가 획득 초 가정", [0, 20, 60, 120], index=0
        )
    input_scores_str = st.text_input(
        "현재까지 추가 입력 점수들 (쉼표로 구분)", value=""
    )
    try:
        input_scores = [
            int(x.strip()) for x in input_scores_str.split(",") if x.strip()
        ]
    except Exception:
        input_scores = []

    current_count = len(filtered[filtered["nickname"] == nickname_for_calc])
    used_attacks = current_count + len(input_scores)
    remaining_attacks = max(0, 10 - used_attacks)

    info = inferred_map.get(
        nickname_for_calc, {"pps": None, "bonus": None, "pairs": []}
    )
    pairs_calc = info.get("pairs", [])
    if not pairs_calc:
        st.info("해당 닉네임의 추정 정보가 불충분하여 계산할 수 없습니다.")
    else:
        rows_calc: List[Dict[str, object]] = []
        per_attack_secs = BASE_SECONDS + int(extra_seconds_assume)
        current_used_score = int(
            filtered[filtered["nickname"] == nickname_for_calc]["score"].sum()
        )
        input_used_score = sum(input_scores) if input_scores else 0
        used_score_total = current_used_score + input_used_score
        for bt, b in pairs_calc:
            pps_calc = int(1000 + 10 * float(bt))
            bonus_calc = int(b)
            # 남은 시간 공식 적용
            time_consumed = (
                (used_score_total - used_attacks * bonus_calc) / pps_calc
                if pps_calc > 0
                else 0
            )
            remain_secs = max(0, int(per_attack_secs - time_consumed))
            # 남은 획득 점수 = 해당 사람의 최종 최대획득점수(선택된 추가초) - (총점+입력합)
            person_max_score = pps_calc * per_attack_secs + 10 * bonus_calc
            remain_score = max(0, person_max_score - used_score_total)
            rows_calc.append(
                {
                    "격전지/추가점수": f"{int(bt) if float(bt).is_integer() else bt}/{b}",
                    "pps": pps_calc,
                    "남은공격횟수": remaining_attacks,
                    "남은시간(초)": remain_secs,
                    "남은획득점수": remain_score,
                }
            )
        st.dataframe(pd.DataFrame(rows_calc), width='stretch')


with tab2:
    st.subheader("총점 상위 15명 시각화 (normal 제외)")
    # normal 데이터는 순위에 영향 주지 않도록 제외
    filtered_no_normal = filtered[
        filtered["boss_order"].astype(str).str.lower() != "normal"
    ]
    rank_df = (
        filtered_no_normal.groupby("nickname")["score"]
        .sum()
        .reset_index()
        .sort_values("score", ascending=False)
        .head(15)
    )
    fig = px.bar(
        rank_df,
        x="score",
        y="nickname",
        orientation="h",
        title="총점 상위 15명",
        text_auto=True,
    )
    fig.update_layout(yaxis={"categoryorder": "total ascending"})
    st.plotly_chart(fig, width='stretch')


with tab3:
    st.subheader("원본 데이터 (보스별 분리)")
    # normal 파일에는 rank가 없을 수 있으므로, 존재하는 컬럼만으로 정렬
    sort_cols = [c for c in ["date", "boss_order", "rank"] if c in filtered.columns]
    grouped = filtered.sort_values(sort_cols).groupby("boss_order")
    for boss_order, g in grouped:
        title = f"보스 {boss_order}번 데이터"
        if str(boss_order).lower() == "normal":
            title = "일반 몬스터(normal) 데이터"
        with st.expander(title):
            # 존재하는 컬럼만 표시하여 normal 데이터도 오류없이 노출
            cols = [
                c
                for c in [
                    "date",
                    "boss_order",
                    "boss_level",
                    "rank",
                    "nickname",
                    "score",
                ]
                if c in g.columns
            ]
            st.dataframe(g[cols], width='stretch')

# 파일 끝
