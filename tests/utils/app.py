import streamlit as st
import pandas as pd

st.title("Simple Streamlit App")

# Create a slider
number = st.slider("Pick a number", 0, 100)

st.write(f"You picked: {number}")

# Show a table
data = pd.DataFrame({
    'x': range(1, 6),
    'y': [n**2 for n in range(1, 6)]
})
st.dataframe(data)
st.line_chart(data)
