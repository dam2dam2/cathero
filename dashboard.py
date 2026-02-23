import os
import json
import re
from typing import Dict, List, Tuple, Optional
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
st.caption("data 디렉토리의 JSON(.txt) 및 CSV 파일로 점수를 분석합니다. (기준: 1wave 및 1.08 배수)")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

BASE_SECONDS = 1200
WAVE_MULTIPLIER = 1.08
BATTLE_MIN = 6.0
BATTLE_MAX = 250.0
BONUS_CANDIDATES = [0, 500, 1000, 1500, 2500, 3000]
EXTRA_SECONDS_CANDIDATES = [0, 20, 60, 120]

# --- 데이터 로딩 함수 ---

@st.cache_data(show_spinner=False)
def load_common_data(guild: str) -> pd.DataFrame:
    """길드별 공통(확정) 데이터를 로드합니다."""
    path = os.path.join(DATA_DIR, guild, "common.csv")
    if not os.path.exists(path):
        return pd.DataFrame(columns=["date", "nickname", "confirmed_bonus", "confirmed_extra", "confirmed_battle"])
    try:
        df = pd.read_csv(path)
        rename_map = {
            "date": "date", "날짜": "date",
            "nickname": "nickname", "닉네임": "nickname",
            "add_score": "confirmed_bonus", "추가점수": "confirmed_bonus",
            "add_second": "confirmed_extra", "추가초": "confirmed_extra", "추가 획득 초": "confirmed_extra",
            "battle_score": "confirmed_battle", "격전지": "confirmed_battle", "격전지점수": "confirmed_battle"
        }
        df.columns = [rename_map.get(c.lower(), c) for c in df.columns]
        
        # 타입 변환
        if "date" in df.columns: df["date"] = df["date"].astype(str)
        if "nickname" in df.columns: df["nickname"] = df["nickname"].astype(str).str.strip()
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
                    "nickname": "nickname", "닉네임": "nickname",
                    "battle_score": "confirmed_battle", "격전지": "confirmed_battle",
                    "add_second": "confirmed_extra", "추가초": "confirmed_extra",
                    "add_score": "confirmed_bonus", "추가점수": "confirmed_bonus"
                }
                df.columns = [rename_map.get(c.lower(), c) for c in df.columns]
                df["date"] = d
                all_rows.append(df)
            except Exception as e:
                st.warning(f"{path} 로드 실패: {e}")
    
    if not all_rows:
        return pd.DataFrame(columns=["date", "nickname", "confirmed_battle", "confirmed_extra", "confirmed_bonus"])
    
    combined_df = pd.concat(all_rows, ignore_index=True)
    if "nickname" in combined_df.columns:
        combined_df["nickname"] = combined_df["nickname"].astype(str).str.strip()
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
                        content = re.sub(r'\]\s*\[', '],[', content)
                        # 전체를 하나의 배열로 감싸서 중첩 리스트 형태로 명시 ([[...],[...]])
                        content = "[" + content + "]"
                        
                        data = json.loads(content)
                        if isinstance(data, dict): data = [data] # 단일 객체 대응
                        for boss_idx, boss_data_list in enumerate(data):
                            # data가 [[player, player], [player, player]] 구조인 경우 (보스 순서대로)
                            if isinstance(boss_data_list, list):
                                for p in boss_data_list:
                                    preview = p.get("preview", {})
                                    rows.append({
                                        "date": date_str, "nickname": str(preview.get("nickname", "Unknown")).strip(),
                                        "score": int(p.get("score", 0)), "updateTime": preview.get("updateTime", ""),
                                        "boss_order": str(boss_idx + 1), "type": "boss"
                                    })
                            else: # 단일 리스트 구조인 경우
                                if isinstance(boss_data_list, dict):
                                    preview = boss_data_list.get("preview", {})
                                    rows.append({
                                        "date": date_str, "nickname": str(preview.get("nickname", "Unknown")).strip(),
                                        "score": int(boss_data_list.get("score", 0)), "updateTime": preview.get("updateTime", ""),
                                        "boss_order": "1", "type": "boss"
                                    })
            except Exception as e:
                st.warning(f"{boss_txt} 로드 실패: {e}")
        elif os.path.exists(boss_csv):
            try:
                bdf = pd.read_csv(boss_csv)
                for _, r in bdf.iterrows():
                    rows.append({
                        "date": date_str, "nickname": str(r.get("nickname", "Unknown")).strip(),
                        "score": int(r.get("score", 0)), "boss_order": str(r.get("boss_order", r.get("order", "1"))),
                        "type": "boss", "updateTime": ""
                    })
            except: pass

        # 일반 몬스터 데이터 처리
        if os.path.exists(normal_txt):
            try:
                with open(normal_txt, "r", encoding="utf-8") as f:
                    content = f.read().strip()
                    if content:
                        # 여러 배열이 연결된 경우 대응
                        content = re.sub(r'\]\s*\[', '],[', content)
                        # 전체를 하나의 배열로 감싸서 중첩 리스트 형태로 명시
                        content = "[" + content + "]"
                        
                        data = json.loads(content)
                        if isinstance(data, dict): data = [data]
                        for p in data:
                            if isinstance(p, dict):
                                preview = p.get("preview", {})
                                rows.append({
                                    "date": date_str, "nickname": str(preview.get("nickname", "Unknown")).strip(),
                                    "score": int(p.get("score", 0)), "updateTime": preview.get("updateTime", ""),
                                    "boss_order": "normal", "type": "normal"
                                })
            except: pass
        elif os.path.exists(normal_csv):
            try:
                ndf = pd.read_csv(normal_csv)
                for _, r in ndf.iterrows():
                    rows.append({
                        "date": date_str, "nickname": str(r.get("nickname", "Unknown")).strip(),
                        "score": int(r.get("score", 0)), "boss_order": "normal",
                        "type": "normal", "updateTime": ""
                    })
            except: pass

    df = pd.DataFrame(rows)
    return df

# --- 계산 및 추정 엔진 ---

BONUS_CANDIDATES = [0, 500, 1000, 1500, 2000, 2500, 3000]

def estimate_battle_score(nickname: str, scores: List[Dict], common_df: pd.DataFrame) -> List[Tuple[float, int]]:
    """닉네임별 데이터를 바탕으로 격전지 점수를 가중치 채점 방식으로 추정합니다."""
    if not scores: return []
    
    # boss 데이터만 추출 (0 이상인 유효 점수만 사용)
    boss_scores = [s for s in scores if s.get("type") == "boss" and s.get("score", 0) > 0]
    if not boss_scores: return []

    candidate_scores = []
    # b_val은 0.5 단위 고정
    b_val_range = [x * 0.5 for x in range(int(BATTLE_MIN * 2), int(BATTLE_MAX * 2) + 1)]
    
    # 1.08은 근사치이므로 주변 범위를 탐색하거나 오차를 허용
    # 여기서는 각 b_val/bonus 조합이 전체 점수들과 얼마나 잘 어울리는지 점수를 매깁니다.
    for b_val in b_val_range:
        wave_p = 1000 + b_val * 10
        for bonus in BONUS_CANDIDATES:
            bonus_val = bonus * 10
            total_match_score = 0
            
            for s_item in boss_scores:
                s = s_item["score"]
                if s < bonus_val: continue
                
                net_score = s - bonus_val
                # 1. 정수 웨이브 여부 확인 (가장 강력한 증거)
                if net_score % wave_p == 0:
                    total_match_score += 10 # 정수 웨이브 일치
                else:
                    # 2. 소수점 웨이브 (약 1.08 비율 근처) 확인
                    # (net_score / wave_p)가 예상 시간 범위 내에 있는지 등
                    waves = net_score / wave_p
                    time_est = waves * 1.08
                    # 1200초(기본) + 추가초 범위 내라면 가능성 있음
                    if 0 < time_est < 1500: # 대략적인 상한선
                        total_match_score += 1 # 소수점 웨이브 가능성
                
            if total_match_score > 0:
                candidate_scores.append(((b_val, bonus), total_match_score))

    # 상위 후보 선정: 1. 일치 점수(내림차순), 2. b_val이 120에 근접한 정도(오름차순)
    candidate_scores.sort(key=lambda x: (-x[1], abs(x[0][0] - 120)))
    
    # 중복 제거 및 상위 3개 추출
    seen = set()
    final_cands = []
    for cand, score in candidate_scores:
        if cand not in seen:
            final_cands.append(cand)
            seen.add(cand)
        if len(final_cands) >= 3: break
        
    return final_cands

# --- 사이드바 설정 ---

guilds_raw = [d for d in os.listdir(DATA_DIR) if os.path.isdir(os.path.join(DATA_DIR, d))]
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
    multi_dates = st.sidebar.multiselect("원하는 날짜들을 선택하세요", dates, default=dates[:1])
    display_dates = multi_dates

# 추가 확정 데이터(score.txt) 로드
score_df_all = load_score_data(sel_guild, display_dates)

# 데이터 필터링
filtered_df = all_data_df[all_data_df["date"].isin(display_dates)]

# --- 탭 구성 ---
tab1, tab2, tab3, tab4, tab5 = st.tabs(["📊 닉네임별 결과", "🏠 길드 합계 / 남은 가능치", "🚫 미참여 현황", "🔍 원본 데이터", "🧮 계산기"])

with tab1:
    st.subheader(f"📅 {', '.join(display_dates) if len(display_dates) < 5 else '선택된 날짜들'} 결과")
    
    nicks = sorted(filtered_df["nickname"].unique())
    results = []
    
    for nick in nicks:
        user_data = filtered_df[filtered_df["nickname"] == nick]
        user_scores = user_data.to_dict("records")
        
        # 추정 수행
        user_common = common_df_all[common_df_all["nickname"] == nick]
        cands = estimate_battle_score(nick, user_scores, common_df_all)
        
        # 확정 값 결정 (우선순위: score.txt > common.csv)
        confirmed_b = None
        confirmed_bonus = None
        confirmed_extra_sec = None
        
        # 1. score.txt (날짜별 개별 확정 데이터) 확인
        if not score_df_all.empty:
            user_score_txt = score_df_all[score_df_all["nickname"] == nick]
            if not user_score_txt.empty:
                # 값이 NaN이 아닌 경우에만 채택
                sb = user_score_txt.iloc[0].get("confirmed_battle")
                se = user_score_txt.iloc[0].get("confirmed_extra")
                so = user_score_txt.iloc[0].get("confirmed_bonus")
                
                if pd.notna(sb): confirmed_b = sb
                if pd.notna(se): confirmed_extra_sec = se
                if pd.notna(so): confirmed_bonus = so

        # 2. common.csv (전체 시트/공통 데이터) 확인
        if not user_common.empty:
            date_match = user_common[user_common["date"].isin(display_dates)]
            if not date_match.empty:
                if confirmed_b is None: confirmed_b = date_match.iloc[0].get("confirmed_battle")
                if confirmed_bonus is None: confirmed_bonus = date_match.iloc[0].get("confirmed_bonus")
                if confirmed_extra_sec is None: confirmed_extra_sec = date_match.iloc[0].get("confirmed_extra")

        # 표시용 값 결정
        b_val = confirmed_b if confirmed_b is not None and not pd.isna(confirmed_b) else (cands[0][0] if cands else 0)
        bonus_val = confirmed_bonus if confirmed_bonus is not None and not pd.isna(confirmed_bonus) else (cands[0][1] if cands else 0)
        
        # 1wave / 1sec 점수 계산
        wave_p = 1000 + b_val * 10
        sec_p = wave_p * WAVE_MULTIPLIER
        
        # 추가 초 및 최대 획득 점수
        total_score = user_data[user_data["score"] > 0]["score"].sum()
        extra_sec = confirmed_extra_sec if confirmed_extra_sec is not None and not pd.isna(confirmed_extra_sec) else 0
        
        def calc_max(esec):
            return int(sec_p * (BASE_SECONDS + esec) + bonus_val * 10)

        max_score = calc_max(extra_sec)
        # 0초 기준으로 총점보다 낮으면 상향 조정 (추정 시)
        if confirmed_extra_sec is None or pd.isna(confirmed_extra_sec):
            for es in EXTRA_SECONDS_CANDIDATES:
                if calc_max(es) >= total_score:
                    extra_sec = es
                    max_score = calc_max(es)
                    break

        results.append({
            "닉네임": nick,
            "공격횟수": len(user_data),
            "총점": int(total_score),
            "평균점수": int(total_score / len(user_data)) if len(user_data) > 0 else 0,
            "격전지점수": b_val,
            "추가점수": int(bonus_val),
            "1wave당 점수": int(wave_p),
            "추가 초": int(extra_sec),
            "최대획득점수": int(max_score)
        })

    res_df = pd.DataFrame(results).sort_values("총점", ascending=False)
    st.dataframe(res_df, use_container_width=True, hide_index=True)

with tab2:
    st.subheader("🏦 길드 성과 요약")
    if not results:
        st.write("데이터가 없습니다.")
    else:
        guild_total = sum(r["총점"] for r in results)
        guild_max = sum(r["최대획득점수"] for r in results)
        guild_remain = guild_max - guild_total
        
        c1, c2, c3 = st.columns(3)
        c1.metric("길드 총점", f"{guild_total:,}")
        c2.metric("최대 획득 가능 점수", f"{guild_max:,}")
        c3.metric("남은 획득 점수", f"{guild_remain:,}")
        
        st.divider()
        st.subheader("⏳ 개인별 남은 가능치")
        remain_list = []
        for r in results:
            remain_score = r["최대획득점수"] - r["총점"]
            pps = r["1wave당 점수"] * WAVE_MULTIPLIER
            
            remain_list.append({
                "닉네임": r["닉네임"],
                "격전지점수": r["격전지점수"],
                "pps (초당)": int(pps),
                "남은 획득 점수": int(remain_score),
                "남은 시간(초) 추정": int(remain_score / pps) if pps > 0 else 0
            })
        st.dataframe(pd.DataFrame(remain_list).sort_values("남은 획득 점수", ascending=False), use_container_width=True, hide_index=True)

with tab3:
    st.subheader("🚫 미참여 현황")
    roster = sorted(list(set(common_df_all["nickname"].unique()) | set(all_data_df["nickname"].unique())))
    
    if mode == "단일 날짜":
        df_date = filtered_df[filtered_df["type"] == "boss"]
        boss_list = sorted(df_date["boss_order"].unique(), key=lambda x: int(x) if x.isdigit() else 999)
        
        miss_counts = {n: 0 for n in roster}
        copy_text_lines = [sel_date, "", "균격 미참여"]
        
        for i, b in enumerate(boss_list):
            participants = set(df_date[df_date["boss_order"] == b]["nickname"])
            missing = [n for n in roster if n not in participants]
            copy_text_lines.append(f"{i+1}. {', '.join(missing) if missing else 'X'}")
            for m in missing: miss_counts[m] += 1
        
        copy_text_lines.append("")
        
        count_groups: Dict[int, List[str]] = {}
        for n, c in miss_counts.items():
            if c > 0:
                count_groups.setdefault(c, []).append(n)
        
        for c in sorted(count_groups.keys()):
            names = count_groups.get(c, [])
            copy_text_lines.append(f"{c}회 미참 : {', '.join(map(str, names))}")
            
        final_copy_text = "\n".join(copy_text_lines)
        st.text_area("복사용 텍스트 (클릭하여 복사 가능)", final_copy_text, height=400)
    else:
        st.info("전체 날짜 합산 미참여 현황")
        df_all_dates = all_data_df[all_data_df["date"].isin(display_dates) & (all_data_df["type"] == "boss")]
        all_miss_counts = {n: 0 for n in roster}
        
        for d in display_dates:
            d_data = df_all_dates[df_all_dates["date"] == d]
            bosses = d_data["boss_order"].unique()
            for b in bosses:
                parts = set(d_data[d_data["boss_order"] == b]["nickname"])
                for n in roster:
                    if n not in parts: all_miss_counts[n] += 1
        
        miss_df = pd.DataFrame([{"닉네임": n, "미참여 합계": c} for n, c in all_miss_counts.items() if c > 0])
        st.dataframe(miss_df.sort_values("미참여 합계", ascending=False), use_container_width=True, hide_index=True)

with tab4:
    st.subheader("📋 전체 원본 데이터")
    st.dataframe(filtered_df.sort_values(["date", "nickname", "updateTime"], ascending=[False, True, False]), use_container_width=True)

with tab5:
    st.subheader("🧮 직접 계산기")
    bc1, bc2, bc3 = st.columns(3)
    c_battle = bc1.number_input("격전지 점수", 6.0, 250.0, 100.0, 0.5)
    c_bonus = bc2.selectbox("추가 점수", BONUS_CANDIDATES)
    c_extra = bc3.selectbox("추가 초", EXTRA_SECONDS_CANDIDATES)
    
    c_wave = 1000 + c_battle * 10
    c_sec = c_wave * WAVE_MULTIPLIER
    c_max = c_sec * (BASE_SECONDS + c_extra) + c_bonus * 10
    
    st.info(f"""
    **계산 결과**
    - 1wave당 점수: **{int(c_wave):,}**
    - 1초당 점수: **{int(c_sec):,}**
    - 최대 획득 점수: **{int(c_max):,}**
    """)
