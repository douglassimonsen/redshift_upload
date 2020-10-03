import pandas


def load_source(source, source_args, source_kwargs):
    if isinstance(source, str):
        if source.endswith('.xlsx'):
            return pandas.read_excel(source, *source_args, **source_kwargs)
        if source.endswith(".csv"):
            return pandas.read_csv(source, *source_args, **source_kwargs)
    if isinstance(source, pandas.DataFrame):
        return source


def upload(source=None, source_args=None, source_kwargs=None):
    source = load_source(source, source_args or [], source_kwargs or {})
