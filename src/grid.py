from dep_tools.grids import grid, gadm
import pandas as pd

grid_gpdf = grid(intersect_with=gadm(), return_type="GeoDataFrame")
grid_gs = grid()
# use for summary products
grid = pd.DataFrame(
    index=grid_gpdf.index,
    data=dict(geobox=[grid_gs.tile_geobox(i) for i in grid_gpdf.index]),
)

