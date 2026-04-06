import streamlit as st
import pandas as pd
import math
import plotly.graph_objects as go

# --- 페이지 설정 (Premium UX) ---
st.set_page_config(
    page_title="⚔️ CATHERO 상층2 격전지 점수 계산기",
    page_icon="⚔️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- 보스 데이터 정의 ---
BOSS_DATA = {
    "바나냥": {"min_lv": 45, "max_lv": 50, "base_lv": 50, "base_hp": 500000},
    "루나": {"min_lv": 35, "max_lv": 40, "base_lv": 40, "base_hp": 450000},
    "루니": {"min_lv": 25, "max_lv": 30, "base_lv": 30, "base_hp": 400000},
    "루이": {"min_lv": 15, "max_lv": 20, "base_lv": 20, "base_hp": 350000},
}

def get_boss_hp(boss_name: str, level: int) -> int:
    """보스 종류와 레벨에 따른 체력을 계산합니다."""
    data = BOSS_DATA[boss_name]
    # 레벨당 5,000씩 감소 (5k per level)
    diff = data["base_lv"] - level
    return data["base_hp"] - (diff * 5000)

# --- 스타일링 (CSS) ---
st.markdown("""
<style>
    .main {
        background-color: #0e1117;
    }
    .stNumberInput, .stSelectbox {
        border-radius: 8px;
    }
    .highlight-card {
        padding: 20px;
        border-radius: 12px;
        background-color: #1e2130;
        border-left: 5px solid #4e73df;
        margin-bottom: 10px;
        color: white;
    }
    .metric-container {
        display: flex;
        justify-content: space-between;
        align-items: center;
        background: rgba(255, 255, 255, 0.05);
        padding: 15px;
        border-radius: 10px;
        margin-bottom: 8px;
    }
    h1, h2, h3 {
        font-family: 'Outfit', sans-serif;
    }
    .stDataFrame {
        border-radius: 10px;
        overflow: hidden;
    }
</style>
""", unsafe_allow_html=True)

# --- 사이드바: 기본 설정 ---
st.sidebar.title("⚔️ 설정")

# 1. 보스 선택
selected_boss = st.sidebar.selectbox("👾 보스 종류", list(BOSS_DATA.keys()))
boss_config = BOSS_DATA[selected_boss]

# 2. 인원수 선택
num_players = st.sidebar.number_input("👥 참여 인원 (2~4명)", min_value=2, max_value=4, value=2)

# --- 메인 대시보드 ---
st.title("⚔️ CATHERO 상층2 격전지 점수 비율 계산기")
st.caption("개별 격전지 점수에 따라 동일한 웨이브 수를 가지도록 보스 체력을 최적으로 배분합니다.")

# --- 메인: 인원별 점수 설정 ---
st.subheader("👤 인원별 격전지 점수 설정")
cols = st.columns(num_players)
player_scores = []
for i in range(num_players):
    with cols[i]:
        score = st.number_input(f"인원 {i+1} 격전지 점수", min_value=0, value=120, key=f"p{i}")
        player_scores.append(score)

# 계산 공통 로직
wave_powers = [1000 + (s * 10) for s in player_scores]
total_wp = sum(wave_powers)

# --- 메인: 각 인원별 비율 표시 ---
st.write("")
st.subheader("📊 인원별 점수 배분 비율")
ratio_cols = st.columns(num_players)
for i in range(num_players):
    ratio = (wave_powers[i] / total_wp) * 100
    with ratio_cols[i]:
        st.metric(f"인원 {i+1} 비중", f"{ratio:.1f}%")

st.divider()

# --- 보스 레벨별 체력 배분 요약표 (한눈에 보기) ---
st.subheader(f"📑 {selected_boss} 레벨별 점수 배분 요약표")
st.write(f"현재 설정된 인원들의 점수를 바탕으로, 보스 레벨별 목표 점수를 한눈에 확인하세요.")

summary_rows = []
for lv in range(boss_config["min_lv"], boss_config["max_lv"] + 1):
    lv_hp = get_boss_hp(selected_boss, lv)
    # 웨이브 수 계산 (모든 인원이 동일한 웨이브를 가짐)
    # W = lv_hp / total_wp
    # Target = ceil(WP * W)
    lv_targets = [math.ceil(lv_hp * (wp / total_wp)) for wp in wave_powers]
    
    row = {"레벨": f"Lv.{lv}", "보스 체력": f"{lv_hp:,}"}
    for i, ts in enumerate(lv_targets):
        row[f"인원 {i+1} 목표"] = f"{ts:,}"
    summary_rows.append(row)

summary_df = pd.DataFrame(summary_rows)
st.table(summary_df)

# --- 하단 안내 ---
st.info("💡 **Tip**: 모든 결과값은 소수점 첫째 자리에서 **올림** 처리되었습니다. 인원별 웨이브 수가 가장 비슷해지는 비율로 계산되었습니다.")
