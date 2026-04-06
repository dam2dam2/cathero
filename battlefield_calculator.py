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

# --- 사이드바: 입력 설정 ---
st.sidebar.title("⚔️ 설정")
st.sidebar.write("상층2 보스 정보를 입력해 주세요.")

# 1. 보스 선택
selected_boss = st.sidebar.selectbox("👾 보스 종류", list(BOSS_DATA.keys()))
boss_config = BOSS_DATA[selected_boss]

# 2. 레벨 선택
selected_lv = st.sidebar.slider(
    f"📊 {selected_boss} 레벨",
    min_value=boss_config["min_lv"],
    max_value=boss_config["max_lv"],
    value=boss_config["max_lv"]
)

# 3. 보스 체력 계산 및 표시
current_hp = get_boss_hp(selected_boss, selected_lv)
st.sidebar.metric("🩸 보스 총 체력", f"{current_hp:,} HP")

st.sidebar.divider()

# 4. 인원수 선택
num_players = st.sidebar.number_input("👥 참여 인원 (2~4명)", min_value=2, max_value=4, value=2)

st.sidebar.write("각 인원의 격전지 점수를 입력하세요:")
player_scores = []
for i in range(num_players):
    score = st.sidebar.number_input(f"👤 인원 {i+1} 격전지 점수", min_value=0, value=120, key=f"p{i}")
    player_scores.append(score)

# --- 메인 대시보드 ---
st.title("⚔️ CATHERO 상층2 격전지 점수 비율 계산기")
st.caption("개별 격전지 점수에 따라 동일한 웨이브 수를 가지도록 보스 체력을 최적으로 배분합니다.")

# 계산 로직
wave_powers = [1000 + (s * 10) for s in player_scores]
total_wp = sum(wave_powers)

# 타겟 점수 배분
target_scores = [math.ceil(current_hp * (wp / total_wp)) for wp in wave_powers]
# 예상 웨이브 수
expected_waves = [round(ts / wp, 2) for ts, wp in zip(target_scores, wave_powers)]

# --- 결과 섹션 ---
col1, col2 = st.columns([1.5, 1])

with col1:
    st.subheader("🎯 배분 결과")
    
    # 결과 테이블 구성
    results_df = pd.DataFrame({
        "구분": [f"인원 {i+1}" for i in range(num_players)],
        "격전지 점수": player_scores,
        "1Wave당 점수": wave_powers,
        "목표 점수 (Score)": [f"{ts:,}" for ts in target_scores],
        "예상 Wave 수": expected_waves
    })
    
    # 가독성을 높인 결과 카드 형태 표시
    for i in range(num_players):
        st.markdown(f"""
        <div class="metric-container">
            <span style="font-size: 1.1em; font-weight: bold; color: #4e73df;">👤 인원 {i+1}</span>
            <span style="font-size: 1.2em; font-weight: bold;">{target_scores[i]:,} 점</span>
            <span style="color: #888;">(예상: {expected_waves[i]} Waves)</span>
        </div>
        """, unsafe_allow_html=True)

    st.write("")
    st.dataframe(results_df, use_container_width=True, hide_index=True)

with col2:
    st.subheader("📊 비율 시각화")
    
    # 링 차트 (Donut Chart)
    fig = go.Figure(data=[go.Pie(
        labels=[f"인원 {i+1}" for i in range(num_players)],
        values=target_scores,
        hole=.4,
        marker=dict(colors=['#4e73df', '#1cc88a', '#36b9cc', '#f6c23e']),
        textinfo='label+percent'
    )])
    
    fig.update_layout(
        margin=dict(t=0, b=0, l=0, r=0),
        legend=dict(orientation="h", yanchor="bottom", y=-0.1, xanchor="center", x=0.5),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(color="white")
    )
    
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# --- 보스 레벨별 체력 배분 요약표 (한눈에 보기) ---
st.subheader(f"📑 {selected_boss} 레벨별 점수 배분 요약표")
st.write(f"현재 설정된 인원별 격전지 점수에 따른 레벨별 목표 점수를 한눈에 확인하세요.")

summary_rows = []
for lv in range(boss_config["min_lv"], boss_config["max_lv"] + 1):
    lv_hp = get_boss_hp(selected_boss, lv)
    lv_targets = [math.ceil(lv_hp * (wp / total_wp)) for wp in wave_powers]
    
    row = {"레벨": f"Lv.{lv}", "보스 체력": f"{lv_hp:,}"}
    for i, ts in enumerate(lv_targets):
        row[f"인원 {i+1} 목표"] = f"{ts:,}"
    summary_rows.append(row)

summary_df = pd.DataFrame(summary_rows)
st.table(summary_df)

# --- 하단 안내 ---
st.info("💡 **Tip**: 인원별 웨이브 수가 가장 비슷해지도록 보스 체력이 배분되었습니다. 인원 1명의 격전지 점수가 바뀌면 모든 인원의 목표 점수가 자동으로 재조정됩니다.")
