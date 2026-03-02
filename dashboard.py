import os
import json
import re
from typing import Dict, List, Tuple, Optional, cast
from datetime import datetime

import pandas as pd
import streamlit as st
import plotly.express as px

# --- 기본 설정 ---
st.set_page_config(
    page_title="CATHERO 점수 계산 대시보드", page_icon="⚔️", layout="wide"
)

# 메인 타이틀 및 캡션
st.title("⚔️ CATHERO 길드 점수 계산 대시보드")
st.caption(
    "data 디렉토리의 JSON(.txt) 및 CSV 파일로 점수를 분석합니다. (기준: 1wave 및 1.08 배수)"
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

BASE_SECONDS = 1200
WAVE_MULTIPLIER = 1.08
BATTLE_MIN = 6.0
BATTLE_MAX = 250.0
BONUS_CANDIDATES = [0, 500, 1000, 1500, 2000, 2500, 3000]
EXTRA_SECONDS_CANDIDATES = [0, 20, 60, 120]

# --- 전역 상수 ---
# WAVE_MULTIPLIER = 1.08  # 이미 상단에 정의됨

# --- 데이터 로딩 함수 ---


@st.cache_data(show_spinner=False)
def load_common_data(guild: str) -> pd.DataFrame:
    """길드별 공통(확정) 데이터를 로드합니다."""
    path = os.path.join(DATA_DIR, guild, "common.csv")
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
        rename_map = {
            "date": "date",
            "날짜": "date",
            "nickname": "nickname",
            "닉네임": "nickname",
            "add_score": "confirmed_bonus",
            "추가점수": "confirmed_bonus",
            "add_second": "confirmed_extra",
            "추가초": "confirmed_extra",
            "추가 획득 초": "confirmed_extra",
            "battle_score": "confirmed_battle",
            "격전지": "confirmed_battle",
            "격전지점수": "confirmed_battle",
        }
        df.columns = [rename_map.get(c.lower(), c) for c in df.columns]

        # 타입 변환
        if "date" in df.columns:
            df["date"] = df["date"].astype(str)
        if "nickname" in df.columns:
            df["nickname"] = df["nickname"].astype(str).str.strip()
        for col in ["confirmed_bonus", "confirmed_extra", "confirmed_battle"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        return df
    except Exception as e:
        st.error(f"common.csv 로드 오류: {e}")
        return pd.DataFrame()


@st.cache_data(show_spinner=False)
def load_score_data(guild: str, dates: List[str]) -> pd.DataFrame:
    """날짜별 score.txt (개별 확정 데이터)를 로드합니다."""
    all_rows = []
    for d in dates:
        path = os.path.join(DATA_DIR, guild, d, "score.txt")
        if os.path.exists(path):
            try:
                # na_values="-"를 사용하여 '-'를 NaN으로 처리
                df = pd.read_csv(path, na_values="-")
                rename_map = {
                    "nickname": "nickname",
                    "닉네임": "nickname",
                    "battle_score": "confirmed_battle",
                    "격전지": "confirmed_battle",
                    "add_second": "confirmed_extra",
                    "추가초": "confirmed_extra",
                    "add_score": "confirmed_bonus",
                    "추가점수": "confirmed_bonus",
                    "range": "target_range",
                    "exclude": "exclude",
                }
                df.columns = [rename_map.get(c.lower(), c) for c in df.columns]
                df["date"] = d
                all_rows.append(df)
            except Exception as e:
                st.warning(f"{path} 로드 실패: {e}")

    if not all_rows:
        return pd.DataFrame(
            columns=[
                "date",
                "nickname",
                "confirmed_battle",
                "confirmed_extra",
                "confirmed_bonus",
                "target_range",
                "exclude",
            ]
        )

    combined_df = pd.concat(all_rows, ignore_index=True)
    if "nickname" in combined_df.columns:
        combined_df["nickname"] = combined_df["nickname"].astype(str).str.strip()

    # exclude T/F 변환 (기본값 F)
    if "exclude" in combined_df.columns:
        combined_df["exclude"] = combined_df["exclude"].apply(
            lambda x: True if str(x).strip().upper() == "T" else False
        )
    else:
        combined_df["exclude"] = False

    return combined_df


@st.cache_data(show_spinner=False)
def load_battle_data(guild: str) -> pd.DataFrame:
    """길드별 실전 데이터를 로드합니다 (.txt JSON 우선, .csv 차선)"""
    guild_dir = os.path.join(DATA_DIR, guild)
    if not os.path.isdir(guild_dir):
        return pd.DataFrame()

    rows = []
    # 날짜별 폴더 탐색
    for date_str in sorted([d for d in os.listdir(guild_dir) if d.isdigit()]):
        date_dir = os.path.join(guild_dir, date_str)

        # boss.txt (JSON) 확인
        boss_txt = os.path.join(date_dir, "boss.txt")
        boss_csv = os.path.join(date_dir, "boss.csv")
        normal_txt = os.path.join(date_dir, "normal.txt")
        normal_csv = os.path.join(date_dir, "normal.csv")

        # 보스 데이터 처리
        if os.path.exists(boss_txt):
            try:
                with open(boss_txt, "r", encoding="utf-8") as f:
                    content = f.read().strip()
                    if content:
                        # 여러 배열이 연결된 경우 대응 (e.g. ][ -> ],[)
                        content = re.sub(r"\]\s*\[", "],[", content)
                        # 전체를 하나의 배열로 감싸서 중첩 리스트 형태로 명시 ([[...],[...]])
                        content = "[" + content + "]"

                        data = json.loads(content)
                        if isinstance(data, dict):
                            data = [data]  # 단일 객체 대응
                        for boss_idx, boss_data_list in enumerate(data):
                            # data가 [[player, player], [player, player]] 구조인 경우 (보스 순서대로)
                            if isinstance(boss_data_list, list):
                                for p in boss_data_list:
                                    preview = p.get("preview", {})
                                    rows.append(
                                        {
                                            "date": date_str,
                                            "nickname": str(
                                                preview.get("nickname", "Unknown")
                                            ).strip(),
                                            "score": int(p.get("score", 0)),
                                            "updateTime": preview.get("updateTime", ""),
                                            "boss_order": str(boss_idx + 1),
                                            "type": "boss",
                                        }
                                    )
                            else:  # 단일 리스트 구조인 경우
                                if isinstance(boss_data_list, dict):
                                    preview = boss_data_list.get("preview", {})
                                    rows.append(
                                        {
                                            "date": date_str,
                                            "nickname": str(
                                                preview.get("nickname", "Unknown")
                                            ).strip(),
                                            "score": int(
                                                boss_data_list.get("score", 0)
                                            ),
                                            "updateTime": preview.get("updateTime", ""),
                                            "boss_order": "1",
                                            "type": "boss",
                                        }
                                    )
            except Exception as e:
                st.warning(f"{boss_txt} 로드 실패: {e}")
        elif os.path.exists(boss_csv):
            try:
                bdf = pd.read_csv(boss_csv)
                for _, r in bdf.iterrows():
                    rows.append(
                        {
                            "date": date_str,
                            "nickname": str(r.get("nickname", "Unknown")).strip(),
                            "score": int(r.get("score", 0)),
                            "boss_order": str(r.get("boss_order", r.get("order", "1"))),
                            "type": "boss",
                            "updateTime": "",
                        }
                    )
            except:
                pass

        # 일반 몬스터 데이터 처리
        if os.path.exists(normal_txt):
            try:
                with open(normal_txt, "r", encoding="utf-8") as f:
                    content = f.read().strip()
                    if content:
                        # 여러 배열이 연결된 경우 대응
                        content = re.sub(r"\]\s*\[", "],[", content)
                        # 전체를 하나의 배열로 감싸서 중첩 리스트 형태로 명시
                        content = "[" + content + "]"

                        data = json.loads(content)
                        if isinstance(data, dict):
                            data = [data]
                        for p in data:
                            if isinstance(p, dict):
                                preview = p.get("preview", {})
                                rows.append(
                                    {
                                        "date": date_str,
                                        "nickname": str(
                                            preview.get("nickname", "Unknown")
                                        ).strip(),
                                        "score": int(p.get("score", 0)),
                                        "updateTime": preview.get("updateTime", ""),
                                        "boss_order": "normal",
                                        "type": "normal",
                                    }
                                )
            except:
                pass
        elif os.path.exists(normal_csv):
            try:
                ndf = pd.read_csv(normal_csv)
                for _, r in ndf.iterrows():
                    rows.append(
                        {
                            "date": date_str,
                            "nickname": str(r.get("nickname", "Unknown")).strip(),
                            "score": int(r.get("score", 0)),
                            "boss_order": "normal",
                            "type": "normal",
                            "updateTime": "",
                        }
                    )
            except:
                pass

        # 추가 보스 데이터 처리 (add_{order}.txt 또는 add_{order}.csv)
        for filename in os.listdir(date_dir):
            match = re.match(r"add_(\d+)\.(txt|csv)", filename)
            if match:
                order = match.group(1)
                file_path = os.path.join(date_dir, filename)
                try:
                    # '.'과 같은 무효 데이터를 처리하기 위해 na_values 설정
                    adf = pd.read_csv(file_path, na_values=[".", "-", ""])

                    # 컬럼명 정규화 먼저 수행
                    rename_map = {
                        "nickname": "nickname",
                        "닉네임": "nickname",
                        "score": "score",
                        "점수": "score",
                    }
                    adf.columns = [
                        rename_map.get(c.lower(), c.lower()) for c in adf.columns
                    ]

                    if "score" in adf.columns:
                        # 점수 컬럼을 숫자로 변환하고 NaN 제거
                        adf["score"] = pd.to_numeric(adf["score"], errors="coerce")
                        adf = adf.dropna(subset=["score"])

                    if "nickname" in adf.columns and not adf.empty:
                        for _, r in adf.iterrows():
                            s_val = r.get("score")
                            if pd.notna(s_val):
                                rows.append(
                                    {
                                        "date": date_str,
                                        "nickname": str(
                                            r.get("nickname", "Unknown")
                                        ).strip(),
                                        "score": int(float(s_val)),
                                        "boss_order": str(order),
                                        "type": "boss",
                                        "updateTime": "",
                                    }
                                )
                except Exception as e:
                    # 로딩 중 오류 발생 시 사용자에게 알림 (디버깅용)
                    st.warning(f"{filename} 로드 실패: {e}")

    df = pd.DataFrame(rows)
    return df


# --- 계산 및 추정 엔진 ---


def estimate_battle_score(
    nickname: str,
    scores: List[Dict],
    common_df: pd.DataFrame,
    exclude_flag: bool = False,
    min_r: float = -1.0,
    max_r: float = 999.0,
) -> List[Tuple[float, int]]:
    """닉네임별 데이터를 바탕으로 격전지 점수를 가중치 채점 방식으로 추정합니다."""
    if not scores:
        return []

    # boss 데이터만 추출 (0 이상인 유효 점수만 사용)
    boss_scores = [
        s for s in scores if s.get("type") == "boss" and s.get("score", 0) > 0
    ]
    if not boss_scores:
        return []

    candidate_scores = []
    # exclude_flag가 True이면 0.1 단위, 아니면 0.5 단위로 후보 생성
    step = 0.1 if exclude_flag else 0.5
    b_val_range = [
        float(int((float(x) * float(step)) * 10 + 0.5) / 10.0)
        for x in range(int(BATTLE_MIN / step), int(BATTLE_MAX / step) + 1)
    ]

    for b_val in b_val_range:
        wave_p = 1000 + b_val * 10
        # range 내에 있는 경우 대폭 가점 부여 (범위 제약 절대화)
        in_tag_range = min_r <= b_val <= max_r

        for bonus in BONUS_CANDIDATES:
            bonus_val = float(bonus) * 10.0
            tm_score: float = 0.0
            if in_tag_range:
                tm_score = tm_score + 50.0

            for s_item in boss_scores:
                s = s_item["score"]
                found_match = False

                # 1.08 역산 및 원본 점수 모두에서 정수 웨이브 확인 (상황별 배율 적용 여부 모호성 대응)
                for multiplier in [float(WAVE_MULTIPLIER), 1.0]:
                    for offset in [-1, 0, 1]:
                        raw_s = int(float(s) / multiplier + 0.5) + offset
                        if raw_s < bonus_val:
                            continue

                        net_score = raw_s - bonus_val
                        if net_score > 0 and net_score % wave_p == 0:
                            # exclude=T이면 모든 정수 웨이브를 강력한 증거로, F이면 5의 배수인 경우만 강력하게.
                            s_str = str(s)
                            if (
                                exclude_flag
                                or s_str.endswith("0")
                                or s_str.endswith("5")
                            ):
                                tm_score = cast(float, tm_score) + 10.0
                            else:
                                tm_score = cast(float, tm_score) + 2.0
                            found_match = True
                            break
                    if found_match:
                        break

                if not found_match:
                    # 정수 웨이브가 아니면 소수점 웨이브 (시간 추정)
                    # 1.08 기준 역산하여 시간대 확인 (raw wave 수와 초는 동일함)
                    raw_s_v = int(float(s) / float(WAVE_MULTIPLIER) + 0.5)
                    net_score_v = float(raw_s_v) - float(bonus_val)
                    if net_score_v > 0:
                        time_est = net_score_v / wave_p
                        if 0.0 < time_est < 1500.0:
                            tm_score = cast(float, tm_score) + 1.0

            cur_tm = cast(float, tm_score)
            if cur_tm > 50.0 or (not in_tag_range and cur_tm > 0):
                candidate_scores.append(((b_val, bonus), cur_tm))

    # Sort: match score descending, then proximity to 120 ascending
    # 매칭 점수(match score)의 가중치를 높이고, 120 근접도(proximity)의 가중치를 낮춤
    # 추가 점수가 0인 경우에 약간의 가산점을 부여하여 우선순위 조정
    def calculate_rank(item):
        (bv, bonus), match_score = item
        # 120 근접도 페널티 (0.1로 낮춤, 기존 0.3)
        proximity_penalty = abs(float(bv) - 120.0) * 0.1
        # 추가 점수가 0인 경우 매칭 점수에 0.5 가점 (우선순위)
        bonus_priority = 0.5 if bonus == 0 else 0.0
        return (
            float(match_score) + bonus_priority - proximity_penalty,
            -abs(float(bv) - 120.0),
        )

    candidate_scores.sort(key=calculate_rank, reverse=True)

    seen = set()
    final_cands = []
    for cand, score in candidate_scores:
        if cand not in seen:
            final_cands.append(cand)
            seen.add(cand)
        if len(final_cands) >= 20:
            break

    return final_cands


# --- 사이드바 설정 ---

guilds_raw = [
    d for d in os.listdir(DATA_DIR) if os.path.isdir(os.path.join(DATA_DIR, d))
]
if not guilds_raw:
    st.error("데이터 디렉토리에 길드 폴더가 없습니다.")
    st.stop()

# BBO-B 우선 배치
guilds = ["BBO-B"] if "BBO-B" in guilds_raw else []
guilds += sorted([g for g in guilds_raw if g != "BBO-B"])

sel_guild = st.sidebar.selectbox("길드 선택", guilds, index=0)

common_df_all = load_common_data(sel_guild)
all_data_df = load_battle_data(sel_guild)

if all_data_df.empty:
    st.info(f"'{sel_guild}' 길드의 데이터가 없습니다.")
    st.stop()

dates = sorted(all_data_df["date"].unique(), reverse=True)
mode = st.sidebar.selectbox("날짜 모드", ["단일 날짜", "전체 날짜(비교)"])

if mode == "단일 날짜":
    sel_date = st.sidebar.selectbox("날짜 선택", dates)
    display_dates = [sel_date]
else:
    multi_dates = st.sidebar.multiselect(
        "원하는 날짜들을 선택하세요", dates, default=[dates[0]] if dates else []
    )
    display_dates = multi_dates

# 추가 확정 데이터(score.txt) 로드
score_df_all = load_score_data(sel_guild, display_dates)

# 데이터 필터링
filtered_df = all_data_df[all_data_df["date"].isin(display_dates)]

# --- 탭 구성 ---
tab1, tab2, tab3, tab4, tab5 = st.tabs(
    [
        "📊 닉네임별 결과",
        "🏠 길드 합계 / 남은 가능치",
        "🚫 미참여 현황",
        "🔍 원본 데이터",
        "🧮 계산기",
    ]
)

with tab1:
    st.subheader(
        f"📅 {', '.join(display_dates) if len(display_dates) < 5 else '선택된 날짜들'} 결과"
    )

    # nicks: data와 score.txt 전체 유저 합집합
    nicks_from_data = set(filtered_df["nickname"].unique())
    nicks_from_score = (
        set(score_df_all["nickname"].unique()) if not score_df_all.empty else set()
    )
    nicks = sorted(list(nicks_from_data | nicks_from_score))
    results = []

    for nick in nicks:
        user_data = filtered_df[filtered_df["nickname"] == nick]
        user_scores = user_data.to_dict("records")

        # 추정 수행
        user_common = common_df_all[common_df_all["nickname"] == nick]

        # score.txt에서 개별 설정(range, exclude) 추출
        target_range_str = "-"
        exclude_flag = False
        min_r, max_r = -1.0, 999.0
        if not score_df_all.empty:
            user_score_txt = score_df_all[score_df_all["nickname"] == nick]
            if not user_score_txt.empty:
                val_range = user_score_txt.iloc[0].get("target_range")
                target_range_str = str(val_range) if pd.notna(val_range) else "-"
                val_exclude = user_score_txt.iloc[0].get("exclude")
                exclude_flag = bool(val_exclude) if pd.notna(val_exclude) else False

                if target_range_str != "-":
                    try:
                        pts = target_range_str.split("-")
                        if len(pts) == 2:
                            min_r, max_r = float(pts[0]), float(pts[1])
                    except:
                        pass

        cands = estimate_battle_score(
            nick,
            user_scores,
            common_df_all,
            exclude_flag=exclude_flag,
            min_r=min_r,
            max_r=max_r,
        )

        # 확정 값 결정 (우선순위: score.txt > common.csv)
        confirmed_b = None
        confirmed_bonus = None
        confirmed_extra_sec = None

        if not score_df_all.empty:
            user_score_txt = score_df_all[score_df_all["nickname"] == nick]
            if not user_score_txt.empty:
                sb = user_score_txt.iloc[0].get("confirmed_battle")
                se = user_score_txt.iloc[0].get("confirmed_extra")
                so = user_score_txt.iloc[0].get("confirmed_bonus")
                if pd.notna(sb):
                    confirmed_b = float(sb)
                if pd.notna(se):
                    confirmed_extra_sec = float(se)
                if pd.notna(so):
                    confirmed_bonus = float(so)

        # 2. common.csv 확인
        if not user_common.empty:
            date_match = user_common[user_common["date"].isin(display_dates)]
            if not date_match.empty:
                if confirmed_b is None:
                    confirmed_b = float(date_match.iloc[0].get("confirmed_battle"))
                if confirmed_bonus is None:
                    confirmed_bonus = float(date_match.iloc[0].get("confirmed_bonus"))
                if confirmed_extra_sec is None:
                    confirmed_extra_sec = float(
                        date_match.iloc[0].get("confirmed_extra")
                    )

        # 총점 및 참여 일수 계산 (최대치 계산용)
        num_days = len(user_data["date"].unique()) if not user_data.empty else 1
        attack_count = len(user_data)
        total_score = user_data[user_data["score"] > 0]["score"].sum()

        # 표시용 값 결정 (우선순위: 확정 데이터 > 단계별 상향 추정)
        # score.txt에 값이 있다면 최우선으로 사용하며, 없을 경우만 추정 후보(cands) 사용
        b_val_base = float(confirmed_b) if confirmed_b is not None else None
        bonus_val_base = float(confirmed_bonus) if confirmed_bonus is not None else None
        extra_sec_base = (
            float(confirmed_extra_sec) if confirmed_extra_sec is not None else None
        )

        # 공식: ((1200 + 추가초) * (1000 + b*10) + bonus*10) * 1.08 (배수 제거)
        def get_max_for_config(cb: float, cbo: float, es: float):
            wp = 1000.0 + cb * 10.0
            return int(((BASE_SECONDS + es) * wp + cbo * 10.0) * WAVE_MULTIPLIER)

        # 추정/선택 로직 시작
        b_val, bonus_val, extra_sec = 0.0, 0.0, 0.0
        scaling_found = False

        # 1. 가능한 모든 후보 조합 (확정값 포함) 생성
        match_candidates = []
        if b_val_base is not None and bonus_val_base is not None:
            match_candidates = [(b_val_base, bonus_val_base)]
        elif cands:
            # 확정된 필드가 있다면 그 필드를 고정하고 cands 필터링
            for c_b, c_bonus in cands:
                if b_val_base is not None and c_b != b_val_base:
                    continue
                if bonus_val_base is not None and c_bonus != bonus_val_base:
                    continue
                match_candidates.append((c_b, c_bonus))

        # 만약 score.txt에 있는데 cands에 없는 경우(우연한 매칭 실패 등)에도 score.txt 보호
        if not match_candidates and (
            b_val_base is not None or bonus_val_base is not None
        ):
            match_candidates = [
                (
                    b_val_base if b_val_base is not None else 120.0,
                    bonus_val_base if bonus_val_base is not None else 0.0,
                )
            ]
        elif not match_candidates:
            match_candidates = [(120.0, 0.0)]

        # 2. 추가 초별로 최적 후보 탐색
        for es_cand in EXTRA_SECONDS_CANDIDATES:
            eb = extra_sec_base
            if eb is not None:
                esc_f = float(es_cand)
                eb_f = float(eb)
                if esc_f != eb_f:
                    continue

            for c_b_f, c_bonus_f in match_candidates:
                calc_m = get_max_for_config(c_b_f, c_bonus_f, float(es_cand))
                if calc_m >= total_score:
                    b_val, bonus_val, extra_sec = c_b_f, c_bonus_f, float(es_cand)
                    scaling_found = True
                    break
            if scaling_found:
                break

        # 3. 모든 시도에도 Max < Total 인 경우 강제 상향 (120초 우선)
        if not scaling_found and match_candidates:
            best_cand = match_candidates[0]
            bc_0 = best_cand[0]
            bc_1 = best_cand[1]
            if bc_0 is not None and bc_1 is not None:
                best_c_b = float(bc_0)
                best_c_bonus = float(bc_1)
            else:
                best_c_b = 120.0
                best_c_bonus = 0.0

            es_b = extra_sec_base
            if es_b is not None:
                curr_es = float(es_b)
            else:
                curr_es = 120.0
            up_step = 0.1 if exclude_flag else 0.5
            curr_b = best_c_b

            while (
                float(get_max_for_config(curr_b, best_c_bonus, curr_es))
                < float(total_score)
                and float(curr_b) < 500.0
            ):
                curr_b = float(int((curr_b + up_step) * 10 + 0.5) / 10.0)
                # round() 대용: x * 10 후 정수화하여 소수점 첫째자리 유지

            b_val, bonus_val, extra_sec = curr_b, best_c_bonus, curr_es

        # 최종 최대 점수 계산 (배수 없이 단일 합산 포텐셜)
        total_max_score = get_max_for_config(b_val, bonus_val, extra_sec)

        results.append(
            {
                "닉네임": str(nick),
                "공격횟수": int(attack_count),
                "총점": int(total_score),
                "평균점수": int(total_score / attack_count) if attack_count > 0 else 0,
                "격전지점수": float(b_val),
                "추가점수": int(bonus_val),
                "1wave당 점수": int(1000 + b_val * 10),
                "추가 초": int(extra_sec),
                "최대획득점수": int(total_max_score),
            }
        )

    res_df = pd.DataFrame(results).sort_values("총점", ascending=False)
    st.dataframe(res_df, use_container_width=True, hide_index=True)

with tab2:
    st.subheader("🏦 길드 성과 요약")
    if results:
        guild_total = int(sum(int(r["총점"]) for r in results))
        guild_max = int(sum(int(r["최대획득점수"]) for r in results))
        guild_remain = int(guild_max - guild_total)

        c1, c2, c3 = st.columns(3)
        c1.metric("길드 총점", f"{guild_total:,}")
        c2.metric("최대 획득 가능 점수", f"{guild_max:,}")
        c3.metric("남은 획득 점수", f"{guild_remain:,}")

        st.divider()
        st.subheader("⏳ 개인별 남은 가능치")
        remain_list = []
        for r in results:
            remain_score = int(r["최대획득점수"]) - int(r["총점"])
            pps = float(r["1wave당 점수"]) * float(WAVE_MULTIPLIER)

            remain_list.append(
                {
                    "닉네임": r["닉네임"],
                    "격전지점수": r["격전지점수"],
                    "pps (초당)": int(pps),
                    "남은 획득 점수": int(remain_score),
                    "남은 시간(초) 추정": int(remain_score / pps) if pps > 0 else 0,
                }
            )
        st.dataframe(
            pd.DataFrame(remain_list).sort_values("남은 획득 점수", ascending=False),
            use_container_width=True,
            hide_index=True,
        )

with tab3:
    st.subheader("🚫 미참여 현황")
    roster = sorted(
        list(
            {str(n) for n in common_df_all["nickname"].unique()}
            | {str(n) for n in all_data_df["nickname"].unique()}
        )
    )

    if mode == "단일 날짜":
        df_date = filtered_df[filtered_df["type"] == "boss"]
        boss_list = sorted(
            df_date["boss_order"].unique(), key=lambda x: int(x) if x.isdigit() else 999
        )

        miss_counts = {}
        for n in roster:
            miss_counts[str(n)] = 0
        copy_text_lines = [sel_date, "", "균격 미참여"]

        for i, b in enumerate(boss_list):
            participants: set = {
                str(n) for n in df_date[df_date["boss_order"] == b]["nickname"]
            }
            missing = [str(n) for n in roster if str(n) not in participants]
            copy_text_lines.append(f"{i+1}. {', '.join(missing) if missing else 'X'}")
            for m in missing:
                m_str = str(m)
                miss_counts[m_str] = miss_counts[m_str] + 1

        copy_text_lines.append("")

        count_groups = {}
        for n, c in miss_counts.items():
            cc = int(c)
            if cc > 0:
                if cc not in count_groups:
                    count_groups[cc] = []
                count_groups[cc].append(str(n))

        for c in sorted(count_groups.keys()):
            names = count_groups.get(c)
            if names is not None:
                copy_text_lines.append(f"{c}회 미참 : {', '.join(map(str, names))}")

        final_copy_text = "\n".join(copy_text_lines)
        st.text_area("복사용 텍스트 (클릭하여 복사 가능)", final_copy_text, height=400)
    else:
        st.info("전체 날짜 합산 미참여 현황")
        df_all_dates = all_data_df[
            all_data_df["date"].isin(display_dates) & (all_data_df["type"] == "boss")
        ]
        all_miss_counts = {}
        for n in roster:
            all_miss_counts[str(n)] = 0

        for d in display_dates:
            d_data = df_all_dates[df_all_dates["date"] == d]
            bosses = d_data["boss_order"].unique()
            for b_id in bosses:
                parts: set = {
                    str(n) for n in d_data[d_data["boss_order"] == b_id]["nickname"]
                }
                for n_name in roster:
                    nn = str(n_name)
                    if nn not in parts:
                        curr_v = all_miss_counts.get(nn, 0)
                        all_miss_counts[nn] = int(curr_v) + 1

        miss_list = []
        for n, c in all_miss_counts.items():
            if int(c) > 0:
                miss_list.append({"닉네임": str(n), "미참여 합계": int(c)})

        miss_df = pd.DataFrame(miss_list)
        if not miss_df.empty:
            st.dataframe(
                miss_df.sort_values("미참여 합계", ascending=False),
                use_container_width=True,
                hide_index=True,
            )

with tab4:
    st.subheader("📋 전체 원본 데이터")
    st.dataframe(
        filtered_df.sort_values(
            ["date", "nickname", "updateTime"], ascending=[False, True, False]
        ),
        use_container_width=True,
    )

with tab5:
    st.subheader("🧮 직접 계산기")
    bc1, bc2, bc3 = st.columns(3)
    c_battle = bc1.number_input("격전지 점수", 6.0, 250.0, 100.0, 0.5)
    c_bonus = bc2.selectbox("추가 점수", BONUS_CANDIDATES)
    c_extra = bc3.selectbox("추가 초", EXTRA_SECONDS_CANDIDATES)

    c_wave = 1000.0 + c_battle * 10.0
    c_sec = c_wave * WAVE_MULTIPLIER
    # 사용자 요청 공식: ((1200 + 추가초) * 1wave점수 + 추가점수*10) * 1.08
    c_max = ((BASE_SECONDS + c_extra) * c_wave + c_bonus * 10.0) * WAVE_MULTIPLIER

    st.info(
        f"""
    **계산 결과**
    - 1wave당 점수: **{int(c_wave):,}**
    - 1초당 점수: **{int(c_sec):,}**
    - 최대 획득 점수: **{int(c_max):,}**
    """
    )
