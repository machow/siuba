import numpy as np

from pandas._libs import lib
from pandas.core.groupby import SeriesGroupBy, DataFrameGroupBy
from pandas import Series

from siuba.experimental.pd_groups.groupby import GroupByAgg, _regroup

from siuba.siu import symbolic_dispatch, strip_symbolic

# Attempt 1, split_apply2 =====================================================

@symbolic_dispatch(cls = SeriesGroupBy)
def split_apply2(col_x, col_y, f, args = tuple(), kwargs = None, is_agg = False) -> SeriesGroupBy:
    """Split-apply-combine over two series from a grouped DataFrame.

    Note: this function requires the apply step (f) to operate on numpy arrays.
    """

    # currently, symbolic_dispatch only handles first argument as a symbol
    col_y = strip_symbolic(col_y)

    assert col_x.grouper is col_y.grouper, "Columns must have the same grouper"

    if kwargs is None:
        kwargs = {}

    # we operate on underlying arrays, so set up splitting up for each column
    splitter_x = col_x.grouper._get_splitter(col_x.obj)
    splitter_y = col_y.grouper._get_splitter(col_y.obj)

    starts, ends = lib.generate_slices(splitter_x.slabels, splitter_x.ngroups)
    
    arr_x = splitter_x._get_sorted_data().values
    arr_y = splitter_y._get_sorted_data().values
    
    # iterate over splits, applying function
    results = []
    for i, (start, end) in enumerate(zip(starts, ends)):
        calc = f(arr_x[start:end], arr_y[start:end], *args, **kwargs)

        # ensure length of each result is correct
        if is_agg:
            if np.ndim(calc) != 0:
                raise ValueError()
        elif len(calc) != end-start:
            raise ValueError()

        results.append(calc)
        
    if is_agg:
        flat = np.array(results)
        index = col_x.grouper.result_index
        return GroupByAgg.from_result(Series(flat, index = index), col_x)

    flat = np.concatenate(results).ravel()
    return _regroup(flat[splitter_x.sort_idx], col_x)


# example corr function ----

@symbolic_dispatch(cls = SeriesGroupBy)
def corr(x, y, method = "pearson", min_periods = 1) -> GroupByAgg:
    from pandas.core.series import nanops
    y = strip_symbolic(y)

    kwargs = dict(method = method, min_periods = min_periods)
    return split_apply2(x, y, nanops.nancorr, kwargs = kwargs, is_agg = True)

@corr.register(Series)
def _corr_ser(x, y, *args, **kwargs) -> Series:
    return x.corr(y, *args, **kwargs)


# Attempt 2, general DataFrame splitting function =============================


class NamedSequence:
    def __init__(self, data, keys):
        self.__key_to_indx = {k: ii for ii, k in enumerate(keys)}

        if len(self.__key_to_indx) > len(keys):
            raise ValueError("Keys must be unique")

        self.__data = tuple(data)

    def __getattr__(self, x):
        ii = self.__key_to_indx[x]
        return self.__data[ii]

    def __getitem__(self, ii):
        return self.__data[ii]

class LazySplit:
    def __init__(self, data, start, end):
        self.__data = data
        self.__start = start
        self.__end = end

    def __getattr__(self, k):
        return getattr(self.__data, k)[self.__start:self.__end]

    def __getitem__(self, ii):
        return self.__data[ii][self.__start:self.__end]

    def __len__(self):
        return self.__end - self.__start


def iter_split(data, splitter = None, array_type = 'values'):
    if splitter is None:
        splitter = data.grouper._get_splitter(data.obj)
    sdata = splitter._get_sorted_data()

    arrays = [getattr(col, array_type) for k, col in sdata.iteritems()]
    named_seq = NamedSequence(arrays, sdata.columns)

    starts, ends = lib.generate_slices(splitter.slabels, splitter.ngroups)
    for i, (start, end) in enumerate(zip(starts, ends)):
        yield LazySplit(named_seq, start, end)

@symbolic_dispatch(cls = DataFrameGroupBy)
def split_apply(data, f, args = tuple(), kwargs = None, is_agg = False):

    call = strip_symbolic(f)

    if kwargs is None:
        kwargs = {}

    results = []
    splitter = data.grouper._get_splitter(data.obj)
    for split in iter_split(data, splitter):
        calc = call(split, *args, **kwargs)

        if np.ndim(calc) == 0:
            # TODO: handle
            pass
        elif is_agg:
            raise ValueError()
        elif len(calc) != len(split):
            raise ValueError()

        results.append(calc)

    if is_agg:
        flat = np.array(results)
        index = data.grouper.result_index
        return GroupByAgg.from_result(Series(flat, index = index), data)

    flat = np.concatenate(results).ravel()
    return _regroup(flat[splitter.sort_idx], data)


# Tests ======================================================================

def test_symbolic_dispatch():
    #from siuba.experimental.split_apply import split_apply2
    import pandas as pd
    df = pd.DataFrame({'g': ['c', 'c', 'a', 'a', 'b'], 'x': [9,8,7,6,5], 'y': [1,2,3,4,5]})
    gdf = df.groupby('g')
    
    res = split_apply2(gdf.x, gdf.y, lambda x,y: x + y)


def test_split_apply_corr():
    #from siuba.experimental.split_apply import corr
    import numpy as np
    import pandas as pd
    
    np.random.seed(123)
    students = pd.DataFrame({
        'student_id': np.repeat(np.arange(2000), 10),
        'course_id': np.random.randint(1, 20, 20000),
        'score': np.random.randint(1, 100, 20000)
    })

    g_stu = students.groupby('student_id')
    corr(g_stu.course_id, g_stu.score)

    


