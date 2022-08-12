from typing import List, Optional
import streamlit as st


def option_select_slider(options: List, selection_string: str) -> Optional[float]:
    if options is None:
        return None
    if len(options) == 1:
        return options[0]

    selection = st.select_slider(
        f'Select {selection_string}',
        options=options)
    return selection
