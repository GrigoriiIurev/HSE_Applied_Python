import streamlit as st
import pandas as pd
from analysis import analyze_city, compute_season_stats, get_current_temperature_sync, check_current_temperature_anomaly
import plotly.graph_objects as go
from datetime import datetime

if "file_loaded" not in st.session_state:
    st.session_state.file_loaded = False

check_columns = {"city", "timestamp", "temperature", "season"}

st.header("Анализ температурных данных")

st.sidebar.header("Параметры")

uploaded_file = st.sidebar.file_uploader(
    "Загрузите файл с историческими данными",
    type=["csv"]
)

if uploaded_file is None:
    st.info("Загрузите CSV-файл, чтобы продолжить.")
    st.stop()


try:
    df = pd.read_csv(uploaded_file)
    missing = check_columns - set(df.columns)
    if missing:
        st.error(f"В файле не хватает колонок: {missing}")
        st.stop()

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    if df["timestamp"].isna().any():
        st.error("Колонка timestamp содержит некорректные значения")
        st.stop()

    if not pd.api.types.is_numeric_dtype(df["temperature"]):
        st.error("Колонка temperature должна быть числовой")
        st.stop()

    if not st.session_state.file_loaded:
        st.toast("Файл успешно загружен")
        st.session_state.file_loaded = True

except Exception as e:
    st.error(f"Ошибка чтения файла: {e}")
    st.stop()

selected_city = st.sidebar.selectbox(
    "Выберите город",
    sorted(df["city"].unique())
)

api_key = st.sidebar.text_input(
    "OpenWeatherMap API Key",
    type="password"
)

df_with_city = df[df["city"] == selected_city].sort_values("timestamp").copy()

if api_key:
    try:
        if selected_city is not None:
            season_stats = compute_season_stats(df)
            current_temp = get_current_temperature_sync(selected_city, api_key)
            is_anomaly = check_current_temperature_anomaly(
                selected_city,
                current_temp,
                season_stats
            )
            if is_anomaly:
                st.write(f"Текущая температура в {selected_city}: {current_temp} °C, температура аномальная")
            else:
                st.write(f"Текущая температура в {selected_city}: {current_temp} °C, температура нормальная")
    except RuntimeError as e:
        if "401" in str(e):
            st.error("Неверный API-ключ OpenWeatherMap")
        else:
            st.error(f"Ошибка при запросе погоды: {e}")
else:
    st.warning("Введите API-ключ OpenWeatherMap")

n_days = len(df_with_city)
start_date = df_with_city["timestamp"].min().date()
end_date = df_with_city["timestamp"].max().date()

start_date = start_date.strftime("%d.%m.%Y")
end_date = end_date.strftime("%d.%m.%Y")

mean_temp = df_with_city["temperature"].mean()
std_temp = df_with_city["temperature"].std()
median_temp = df_with_city["temperature"].median()

min_idx = df_with_city["temperature"].idxmin()
max_idx = df_with_city["temperature"].idxmax()

min_temp = df_with_city.loc[min_idx, "temperature"]
min_date = df_with_city.loc[min_idx, "timestamp"].date().strftime("%d.%m.%Y")

max_temp = df_with_city.loc[max_idx, "temperature"]
max_date = df_with_city.loc[max_idx, "timestamp"].date().strftime("%d.%m.%Y")

st.subheader(f"Описательная статистика для города {selected_city}")

st.markdown(
    f"""
    **Период наблюдений:** с {start_date} по {end_date}  
    **Количество наблюдений:** {n_days} дней  

    **Средняя температура:** {mean_temp:.2f} °C  
    **Стандартное отклонение:** {std_temp:.2f} °C  
    **Медиана:** {median_temp:.2f} °C  

    **Минимальная температура:** {min_temp:.2f} °C  
    **Дата:** {min_date}  

    **Максимальная температура:** {max_temp:.2f} °C  
    **Дата:** {max_date}
    """
)

df_city_an = analyze_city(df_with_city, window=30)

st.subheader("Фильтр временного диапазона")

period_type = st.radio(
    "Какой период показать?",
    ["Год", "Месяц", "Сезон", "Произвольный период"],
    horizontal=True
)

df_plot = df_city_an.copy()

df_plot["year"] = df_plot["timestamp"].dt.year
df_plot["month"] = df_plot["timestamp"].dt.month

if period_type == "Год":
    years = sorted(df_plot["year"].unique())
    selected_year = st.selectbox("Выберите год", years)
    df_plot = df_plot[df_plot["year"] == selected_year]

elif period_type == "Месяц":
    years = sorted(df_plot["year"].unique())
    selected_year = st.selectbox("Год", years)

    selected_month = st.selectbox(
        "Месяц",
        list(range(1, 13)),
        format_func=lambda x: datetime(2000, x, 1).strftime("%B")
    )

    df_plot = df_plot[
        (df_plot["year"] == selected_year) &
        (df_plot["month"] == selected_month)
    ]

elif period_type == "Сезон":
    years = sorted(df_plot["year"].unique())
    selected_year = st.selectbox("Год", years)

    seasons = ["winter", "spring", "summer", "autumn"]
    selected_season = st.selectbox("Сезон", seasons)

    df_plot = df_plot[
        (df_plot["year"] == selected_year) &
        (df_plot["season"] == selected_season)
    ]

else:
    min_date = df_plot["timestamp"].min().date()
    max_date = df_plot["timestamp"].max().date()

    date_range = st.date_input(
        "Выберите период",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date
    )

    if len(date_range) == 2:
        start_date, end_date = date_range
        df_plot = df_plot[
            (df_plot["timestamp"] >= pd.to_datetime(start_date)) &
            (df_plot["timestamp"] <= pd.to_datetime(end_date))
        ]

only_anomalies = st.checkbox("Показывать только аномальные точки", value=False)


fig = go.Figure()

if not only_anomalies:
    fig.add_trace(go.Scatter(
        x=df_plot["timestamp"],
        y=df_plot["temperature"],
        mode="lines+markers",
        name="Температура"
    ))

fig.add_trace(go.Scatter(
    x=df_plot["timestamp"],
    y=df_plot["upper_bound"],
    mode="lines",
    name="Верхняя граница",
    line=dict(dash="dash")
))

fig.add_trace(go.Scatter(
    x=df_plot["timestamp"],
    y=df_plot["lower_bound"],
    mode="lines",
    name="Нижняя граница",
    line=dict(dash="dash")
))

fig.update_layout(
    legend=dict(
        orientation="h",
        yanchor="bottom",
        y=1.02,
        xanchor="center",
        x=0.5
    )
)

df_anom = df_plot[df_plot["is_anomaly"] == True]

fig.add_trace(go.Scatter(
    x=df_anom["timestamp"],
    y=df_anom["temperature"],
    mode="markers",
    name="Аномалии",
    marker=dict(
        color="red",
        size=8,
        symbol="circle"
    )
))

st.plotly_chart(fig, use_container_width=True)

season_stats = compute_season_stats(df)
city_season_stats = season_stats[season_stats["city"] == selected_city]

st.subheader("Сезонный профиль температуры")
st.dataframe(city_season_stats)
