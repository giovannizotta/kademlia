from typing import List, Optional, Tuple
import streamlit as st


def time_slider(max_time: int) -> Tuple[float, float]:
    start_time, end_time = st.select_slider(
        'Select a time interval',
        options=list(range(0, max_time+1, max_time // 100)),
        value=(0, max_time))
    return start_time, end_time


def select_slider(options: List, selection_string: str) -> Optional[float]:
    if options is None:
        return None
    if len(options) == 1:
        return options[0]

    selection = st.select_slider(
        f'Select {selection_string}',
        options=options)
    return selection
