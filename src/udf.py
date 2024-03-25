
import pyarrow as pa
import itertools

def register_udfs():
    pass

# TODO: turn udf_input_iter into a decorator IF POSSIBLE, returning pa.lib.Table.from_pylist(out)
"""
def normalize_city_name(_city : str) -> str:
    out = []
    for city in udf_input_iter([_city]):
        #print('city yielding from udf_input_iter inside normalize_city_name: ', city)
        if not city:
            out.append({'city' : ''})
            continue
        city_norm  = ' '.join(c.lower().capitalize() for c in city.split(' '))
        out.append({'city' : city_norm})
    return pa.lib.Table.from_pylist(out)      
"""

def udf_input_iter(udf_inputs : list[pa.lib.ChunkedArray]) -> iter:
    for i in range(len(udf_inputs)):
        udf_inputs[i] = udf_inputs[i].combine_chunks()
    table = pa.lib.Table.from_arrays(
        udf_inputs, 
        names=[f'c{i}' for i in range(len(udf_inputs))]
    )
    args = [list(itertools.chain(v)) for v in table.to_pydict().values()]
    for zarg in list(zip(*args)):
        if len(zarg) == 1:
            yield zarg[0]
        else:
            yield zarg