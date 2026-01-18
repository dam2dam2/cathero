import os
from typing import Dict, List, Tuple, Optional

import pandas as pd
import streamlit as st
import plotly.express as px


st.set_page_config(
    page_title="CATHERO 점수 계산 대시보드", page_icon="⚔️", layout="wide"
)
st.title("⚔️ CATHERO 길드 점수 계산 대시보드")
st.caption("data_csv의 CSV로 격전지/추가점수/최대점수를 확정/추정합니다.")


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ALT_DATA_DIR = os.path.join(BASE_DIR, "data_csv")

BASE_SECONDS = 1200
BATTLE_MIN_DEFAULT = 6.0
BATTLE_MAX_DEFAULT = 250.0
BONUS_CANDIDATES_DEFAULT = [0, 500, 1000, 1500, 2500, 3000]


@st.cache_data(show_spinner=False)
def load_common_for_guild(guild: str) -> pd.DataFrame:
    path = os.path.join(ALT_DATA_DIR, guild, "common.csv")
    if not os.path.exists(path):
        return pd.DataFrame(
            columns=[
                "date",
                "nickname",
                "confirmed_bonus",
                "confirmed_extra",
                "confirmed_battle",
            ]
        )
    try:
        df = pd.read_csv(path)
        if df.empty:
            return pd.DataFrame(
                columns=[
                    "date",
                    "nickname",
                    "confirmed_bonus",
                    "confirmed_extra",
                    "confirmed_battle",
                ]
            )
        cols_lower = {c.lower(): c for c in df.columns}

        def pick(*cands):
            for k in cands:
                if k in cols_lower:
                    return cols_lower[k]
            return None

        date_col = pick("date", "날짜")
        nick_col = pick("nickname", "닉네임")
        add_score_col = pick("add_score", "추가점수")
        add_second_col = pick("add_second", "추가초", "추가 획득 초")
        battle_col = pick("battle_score", "격전지", "격전지점수")

        rename_map = {}
        if date_col:
            rename_map[date_col] = "date"
        if nick_col:
            rename_map[nick_col] = "nickname"
        if add_score_col:
            rename_map[add_score_col] = "confirmed_bonus"
        if add_second_col:
            rename_map[add_second_col] = "confirmed_extra"
        if battle_col:
            rename_map[battle_col] = "confirmed_battle"
        df = df.rename(columns=rename_map)
        keep = [
            c
            for c in [
                "date",
                "nickname",
                "confirmed_bonus",
                "confirmed_extra",
                "confirmed_battle",
            ]
            if c in df.columns
        ]
        df = df[keep]
        if "date" in df.columns:
            df["date"] = df["date"].astype(str)
        df["nickname"] = (
            df.get("nickname", pd.Series(dtype=str)).astype(str).str.strip()
        )
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
                "date",
                "nickname",
                "confirmed_bonus",
                "confirmed_extra",
                "confirmed_battle",
            ]
        )


@st.cache_data(show_spinner=False)
def load_all_scores(guild: str) -> pd.DataFrame:
    base = os.path.join(ALT_DATA_DIR, guild)
    rows: List[Dict[str, object]] = []
    if not os.path.isdir(base):
        return pd.DataFrame(
            columns=[
                "guild",
                "date",
                "boss_order",
                "boss_level",
                "rank",
                "nickname",
                "score",
            ]
        )
    for d in sorted([x for x in os.listdir(base) if x.isdigit()]):
        ddir = os.path.join(base, d)
        boss_path = os.path.join(ddir, "boss.csv")
        normal_path = os.path.join(ddir, "normal.csv")
        if os.path.exists(boss_path):
            try:
                bdf = pd.read_csv(boss_path)
                for _, r in bdf.iterrows():
                    rows.append(
                        {
                            "guild": r.get("guild", guild),
                            "date": str(r.get("date", d)),
                            "boss_order": r.get("boss_order", r.get("order", "")),
                            "boss_level": r.get("boss_level", r.get("level", "")),
                            "rank": r.get("rank", None),
                            "nickname": str(r.get("nickname", "")).strip(),
                            "score": int(r.get("score", 0)),
                        }
                    )
            except Exception as e:
                st.warning(f"보스 데이터 로드 실패: {boss_path} ({e})")
        if os.path.exists(normal_path):
            try:
                ndf = pd.read_csv(normal_path)
                for _, r in ndf.iterrows():
                    rows.append(
                        {
                            "guild": guild,
                            "date": str(r.get("date", d)),
                            "boss_order": "normal",
                            "boss_level": "",
                            "rank": None,
                            "nickname": str(r.get("nickname", "")).strip(),
                            "score": int(r.get("score", 0)),
                        }
                    )
            except Exception as e:
                st.warning(f"일반 데이터 로드 실패: {normal_path} ({e})")
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["date"] = df["date"].astype(str)
    df["boss_order"] = df["boss_order"].astype(str)
    return df


def choose_top_pairs(
    candidates: List[Tuple[float, int]], nickname: str, common_df_all: pd.DataFrame
) -> List[Tuple[float, int]]:
    if not candidates:
        return []
    commons = common_df_all[common_df_all["nickname"].astype(str) == str(nickname)]
    if commons.empty:
        return sorted(candidates, key=lambda x: x[0], reverse=True)[:2]
    target_battles = commons["confirmed_battle"].dropna().astype(float).tolist()
    target_bonus = commons["confirmed_bonus"].dropna().astype(int).tolist()

    def dist(pair: Tuple[float, int]) -> float:
        bt, bn = pair
        db = min([abs(bt - t) for t in target_battles], default=0.0)
        dn = min([abs(bn - t) for t in target_bonus], default=0.0)
        return db + dn / 1000.0

    return sorted(candidates, key=lambda x: (dist(x), -x[0]))[:2]


def infer_candidates(scores: List[int]) -> List[Tuple[float, int]]:
    cand: List[Tuple[float, int]] = []
    for b2 in range(int(BATTLE_MIN_DEFAULT * 2), int(BATTLE_MAX_DEFAULT * 2) + 1):
        battle = b2 / 2.0
        pps = int(1000 + 10 * float(battle))
        for bonus in BONUS_CANDIDATES_DEFAULT:
            ok = True
            for sc in scores:
                if sc == int(bonus):
                    continue
                diff = int(sc) - int(bonus)
                if diff < 0 or diff % pps != 0:
                    ok = False
                    break
            if ok:
                cand.append((battle, int(bonus)))
    if cand:
        return cand
    if scores:
        non_bonus = [s for s in scores if s not in BONUS_CANDIDATES_DEFAULT]
        if non_bonus:
            valid5 = [s for s in non_bonus if s % 5 == 0]
            m = min(valid5) if valid5 else min(non_bonus)
        else:
            m = min(scores)
        for b2 in range(int(BATTLE_MIN_DEFAULT * 2), int(BATTLE_MAX_DEFAULT * 2) + 1):
            battle = b2 / 2.0
            pps = int(1000 + 10 * float(battle))
            for bonus in BONUS_CANDIDATES_DEFAULT:
                if m == int(bonus):
                    cand.append((battle, int(bonus)))
                    break
                diff = int(m) - int(bonus)
                if diff >= 0 and diff % pps == 0:
                    cand.append((battle, int(bonus)))
                    break
    return cand


# Sidebar
guilds = [
    d for d in os.listdir(ALT_DATA_DIR) if os.path.isdir(os.path.join(ALT_DATA_DIR, d))
]
sel_guild = st.sidebar.selectbox("길드 선택", sorted(guilds)) if guilds else None

if sel_guild is None:
    st.info("data_csv 하위에 길드 폴더가 없습니다.")
    st.stop()

common_df_all = load_common_for_guild(sel_guild)
all_df = load_all_scores(sel_guild)
if all_df.empty:
    st.info("데이터가 없습니다.")
    st.stop()

dates = sorted(all_df["date"].unique())
sel_date = st.sidebar.selectbox("날짜 선택", ["전체"] + dates, index=0)

filtered = all_df if sel_date == "전체" else all_df[all_df["date"] == str(sel_date)]

tab1, tab2, tab3, tab4 = st.tabs(
    ["닉네임 추정 결과", "시각화", "원본 데이터", "🧮 계산기"]
)


with tab1:
    st.subheader("닉네임별 결과")
    common_by_key: Dict[Tuple[str, str], Dict[str, Optional[float]]] = {}
    for _, r in common_df_all.iterrows():
        key = (str(r.get("nickname", "")).strip(), str(r.get("date", "")))
        common_by_key[key] = {
            "battle": (
                float(r["confirmed_battle"])
                if pd.notna(r.get("confirmed_battle"))
                else None
            ),
            "bonus": (
                int(r["confirmed_bonus"])
                if pd.notna(r.get("confirmed_bonus"))
                else None
            ),
            "extra": (
                int(r["confirmed_extra"])
                if pd.notna(r.get("confirmed_extra"))
                else None
            ),
        }

    nicknames = sorted(filtered["nickname"].dropna().astype(str).unique())
    rows: List[Dict[str, object]] = []
    inferred: Dict[str, Dict[str, object]] = {}
    for nick in nicknames:
        g = filtered[filtered["nickname"].astype(str) == str(nick)]
        total = int(g["score"].sum())
        attacks = int(len(g))
        avg = int(round(total / attacks)) if attacks > 0 else 0

        ckey = (nick, str(sel_date))
        confirmed = common_by_key.get(ckey, {}) if sel_date != "전체" else {}
        c_battle = confirmed.get("battle")
        c_bonus = confirmed.get("bonus")
        c_extra = confirmed.get("extra")

        scores_list = g["score"].astype(int).tolist()
        candidates = infer_candidates(scores_list)
        top2 = choose_top_pairs(candidates, nick, common_df_all)

        if c_battle is not None:
            pps = int(1000 + 10 * float(c_battle))
            b_use = int(c_bonus) if c_bonus is not None else 0
            ex_use = int(c_extra) if c_extra is not None else 0
        elif top2:
            bt, bn = top2[0]
            pps = int(1000 + 10 * float(bt))
            b_use = int(bn)
            ex_use = 0
        else:
            pps = 0
            b_use = 0
            ex_use = 0

        def max_points(extra: int) -> int:
            return int(pps) * int(BASE_SECONDS + extra) + 10 * int(b_use)

        max_score = max_points(ex_use) if pps > 0 else 0
        if pps > 0 and c_battle is None:
            if max_score < total:
                for ex_try in [20, 60, 120]:
                    if max_points(ex_try) >= total:
                        ex_use = ex_try
                        max_score = max_points(ex_use)
                        break

        inferred[nick] = {
            "pps": pps,
            "bonus": b_use,
            "extra": ex_use,
            "pairs": top2,
            "valid": pps > 0,
            "total": total,
        }

        rows.append(
            {
                "닉네임": nick,
                "공격횟수": attacks,
                "총점": total,
                "평균점수": avg,
                "확정 격전지점수": (
                    int(c_battle)
                    if c_battle is not None and float(c_battle).is_integer()
                    else (c_battle if c_battle is not None else "-")
                ),
                "확정 추가점수": (int(c_bonus) if c_bonus is not None else "-"),
                "확정 1초당 점수": (
                    int(1000 + 10 * float(c_battle)) if c_battle is not None else "-"
                ),
                "확정 추가 초": (
                    int(c_extra)
                    if c_extra is not None
                    else (ex_use if c_battle is None and pps > 0 else "-")
                ),
                "최대획득점수": (max_score if pps > 0 else "-"),
                "추정 격전지/추가점수": (
                    ", ".join(
                        [
                            f"{int(bt) if float(bt).is_integer() else bt}/{bn}"
                            for bt, bn in top2
                        ]
                    )
                    if top2
                    else ("추정불가" if c_battle is None else "-")
                ),
            }
        )

    out_df = pd.DataFrame(rows).astype(str)
    st.dataframe(out_df, use_container_width=True)

    if sel_date != "전체":
        st.divider()
        st.subheader("길드 합계")
        included = [n for n, info in inferred.items() if info.get("valid")]
        excluded = [n for n in inferred.keys() if n not in included]
        guild_total = int(filtered["score"].sum())
        included_total = int(
            filtered[filtered["nickname"].isin(included)]["score"].sum()
        )
        excluded_total = int(
            filtered[~filtered["nickname"].isin(included)]["score"].sum()
        )
        guild_est_max = 0
        for n in included:
            info = inferred[n]
            pps_i = int(info.get("pps", 0))
            bn_i = int(info.get("bonus", 0))
            ex_i = int(info.get("extra", 0))
            if pps_i <= 0:
                continue
            guild_est_max += pps_i * (BASE_SECONDS + ex_i) + 10 * bn_i
        guild_remaining = max(0, guild_est_max - included_total)
        c1, c2 = st.columns(2)
        with c1:
            st.metric("길드 총점", f"{guild_total:,}")
        with c2:
            st.metric("길드 추정 최대획득점수", f"{guild_est_max:,}")
        c3, c4 = st.columns(2)
        with c3:
            st.metric("길드 남은 획득점수(추정 가능 인원)", f"{guild_remaining:,}")
        with c4:
            st.metric("추정불가 인원 총점", f"{excluded_total:,}")
        c5, c6 = st.columns(2)
        with c5:
            st.metric("추정 가능 인원 수", f"{len(included):,}")
        with c6:
            st.metric("추정 불가 인원 수", f"{len(excluded):,}")
        with st.expander("인원 상세(포함/제외)"):
            cc1, cc2 = st.columns(2)
            with cc1:
                st.markdown("**포함 인원(추정 가능)**")
                st.write(", ".join(included) if included else "-")
                st.write(f"포함 총점: {included_total:,}")
            with cc2:
                st.markdown("**제외 인원(추정 불가)**")
                st.write(", ".join(excluded) if excluded else "-")
        st.divider()
        st.subheader("닉네임별 남은 가능치(pps/남은횟수/시간/점수)")
        remain_rows: List[Dict[str, object]] = []
        for n in included:
            info = inferred[n]
            pps_i = int(info.get("pps", 0))
            bn_i = int(info.get("bonus", 0))
            ex_i = int(info.get("extra", 0))
            total_i = int(info.get("total", 0))
            max_i = pps_i * (BASE_SECONDS + ex_i) + 10 * bn_i
            remain_score = max(0, max_i - total_i)
            g_nick = filtered[filtered["nickname"].astype(str) == str(n)]
            used_attacks_with_time = int(
                (g_nick["score"].astype(int) != int(bn_i)).sum()
            )
            battle_val = (pps_i - 1000) / 10.0
            time_used = used_attacks_with_time * battle_val
            total_time = BASE_SECONDS + ex_i
            time_left = max(0, total_time - time_used)
            remain_attacks = int(time_left // battle_val) if battle_val > 0 else 0
            time_left_formula = (
                max(0, (remain_score - remain_attacks * int(bn_i)) / pps_i)
                if pps_i > 0
                else 0
            )
            remain_rows.append(
                {
                    "닉네임": n,
                    "격전지": (
                        int(battle_val)
                        if float(battle_val).is_integer()
                        else float(battle_val)
                    ),
                    "pps": pps_i,
                    "추가점수": bn_i,
                    "추가초": ex_i,
                    "남은공격횟수": remain_attacks,
                    "남은시간(초)": int(round(time_left_formula)),
                    "남은획득점수": int(remain_score),
                }
            )
        remain_df = pd.DataFrame(remain_rows)
        st.dataframe(
            remain_df.sort_values(["남은획득점수"], ascending=False),
            use_container_width=True,
        )

    if sel_date == "전체":
        st.divider()
        st.subheader("닉네임별 날짜 비교: 확정/추정 격전지/추가점수")
        dates_all = sorted(filtered["date"].unique())
        compare_rows: List[Dict[str, object]] = []
        for nick in nicknames:
            row = {"닉네임": nick}
            for d in dates_all:
                g2 = filtered[
                    (filtered["nickname"].astype(str) == str(nick))
                    & (filtered["date"] == d)
                ]
                if g2.empty:
                    row[d] = "-"
                    continue
                key = (nick, str(d))
                cb = common_by_key.get(key, {}).get("battle")
                bo = common_by_key.get(key, {}).get("bonus")
                if cb is not None:
                    val_b = int(cb) if float(cb).is_integer() else float(cb)
                    val_o = int(bo) if bo is not None else 0
                    row[d] = f"{val_b}/{val_o}"
                    continue
                scores2 = g2["score"].astype(int).tolist()
                cand2 = infer_candidates(scores2)
                top2d = choose_top_pairs(cand2, nick, common_df_all)
                row[d] = (
                    ", ".join(
                        [
                            f"{int(bt) if float(bt).is_integer() else bt}/{bn}"
                            for bt, bn in top2d
                        ]
                    )
                    if top2d
                    else "추정불가"
                )
            compare_rows.append(row)
        compare_df = pd.DataFrame(compare_rows).astype(str)
        st.dataframe(compare_df, use_container_width=True)


with tab2:
    st.subheader("총점 상위 15명 시각화 (normal 제외)")
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
    st.plotly_chart(fig, use_container_width=True)


with tab3:
    st.subheader("원본 데이터 (보스별 분리)")
    sort_cols = [c for c in ["date", "boss_order", "rank"] if c in filtered.columns]
    grouped = filtered.sort_values(sort_cols).groupby("boss_order")
    for boss_order, g in grouped:
        title = f"보스 {boss_order}번 데이터"
        if str(boss_order).lower() == "normal":
            title = "일반 몬스터(normal) 데이터"
        with st.expander(title):
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
            st.dataframe(g[cols], use_container_width=True)


with tab4:
    st.subheader("최대 획득 점수 계산기")
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
    max_score_calc = int(picked_pps) * int(total_secs) + 10 * int(total_bonus)
    st.markdown(f"결과: pps {int(picked_pps):,} / 최대 {int(max_score_calc):,}점")
