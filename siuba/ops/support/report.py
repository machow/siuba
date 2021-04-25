import pandas as pd

ALL_BACKENDS = ["pandas", "postgresql", "mysql", "redshift"]
OUT_COLS = ["category", "name", "arity", *ALL_BACKENDS]
EXCLUDE_CATS = ["_special_methods"]

def _left_align(df):
    left_aligned_df = df.style.set_properties(**{'text-align': 'left'})
    left_aligned_df = left_aligned_df.set_table_styles(
        [dict(selector='th', props=[('text-align', 'left')])]
    )
    return left_aligned_df


def get_support_table():
    from siuba.ops.support import spec

    df_spec = (pd.DataFrame(spec).T
            .drop(columns = "name")
            .reset_index()
            .rename(columns = {"index": "name"})
            )

    for backend in ALL_BACKENDS:
        df_spec[backend] = df_spec.backends.transform(lambda x: x[backend]['is_supported'])

    # remove extra columns, and rows for unplanned methods
    final_spec = (df_spec
            .loc[lambda d: d.kind.notna() & ~d.category.isin(EXCLUDE_CATS), :]
            .loc[:, OUT_COLS]
            )

    final_spec[ALL_BACKENDS] = final_spec[ALL_BACKENDS].replace({True: "✅", False: ""})

    # sort it all out
    sorted_spec = (final_spec
            .sort_values(["category", "name"])
            .assign(category = lambda d: d.category.where(~d.category.duplicated(), ""))
            )

    return _left_align(sorted_spec).hide_index()

