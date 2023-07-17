# %%
import pandas as pd

# %%
df = (
    pd.read_json("example_data.json")
    .sort_values("ordertime")
    .assign(**{"ordertime": lambda x: pd.to_datetime(x["ordertime"], unit="ms")})
    .reset_index(drop=True)
)
# %%
df.head()
# %%
_ = df.plot(x="ordertime", y="orderunits")
# %%
