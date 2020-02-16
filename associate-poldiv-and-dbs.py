import concurrent.futures
import json

# import matplotlib.pyplot as plt
import shapefile
from shapely.errors import TopologicalError
from shapely.geometry import Polygon
from tqdm import tqdm

from toolbox import generic_esri_reader, time_for_filename, test_directory


def associate_poldiv_and_dbs(prov_id=35, multiprocess=False):
    pol_divs = "./geo_data/polling_divisions_boundaries_2015_shp/poll_div_bounds_2015.shp"
    diss_block = "./geo_data/dissemination_blocks_cartographic/ldb_000b16a_e.shp"
    print("Reading files")
    pol_divs = generic_esri_reader(pol_divs)
    diss_block = generic_esri_reader(diss_block)
    print(f"Filtering by Province ID {prov_id}")
    pol_divs = [div for div in tqdm(pol_divs, desc="Filtering Poldivs") if
                prov_id * 1000 <= div.get('fed_num', 0) < (prov_id + 1) * 1000]
    diss_block = [db for db in tqdm(diss_block, desc="Filtering diss blocks") if db.get('pruid') == str(prov_id)]
    diss_block_Polygons = {db.get("dbuid"): (build_shape(db), list(db.get("shape").bbox)) for db in
                           tqdm(diss_block, desc="Creating Initial Polygons for Diss Blocks")}
    assert len(diss_block) == len(diss_block_Polygons)
    results = dict()
    if multiprocess:
        # todo figure out why this seems to time out
        results = get_diss_blocks_mp(pol_divs, diss_block_Polygons)
    else:
        for PolDiv in tqdm(pol_divs, desc="Assigning PolDivs"):
            dbs_in_pol_div, _ = get_diss_blocks(PolDiv, diss_block_Polygons)
            results[get_pol_div_str(PolDiv)] = dbs_in_pol_div
    write_association_file(results, outfile=f"./output/PolDiv_DB_association_prov_{prov_id}_{time_for_filename()}.json")
    print("Fin")


def get_diss_blocks_mp(pol_divs, diss_block_Polygons):
    print("Running with Multiprocessing")
    associations = dict()
    args = [dict(PolDiv=PolDiv, Polygons_diss_block=diss_block_Polygons)
            for PolDiv in tqdm(pol_divs, desc="Creating MP Arguments")]
    # args = [(PolDiv, {dbid: db for dbid, db in diss_block_Polygons.items()
    #                   if do_bounding_boxes_overlap(db[1], PolDiv['shape'].bbox)})
    #         for PolDiv in tqdm(pol_divs, desc="Creating MP Arguments")]
    with concurrent.futures.ProcessPoolExecutor() as executor:
        results = [executor.submit(get_diss_blocks_wrapper, arg) for arg in args]
        pbar = tqdm(desc="Getting Assigned PolDivs", total=len(pol_divs))
        for result in concurrent.futures.as_completed(results):
            dbs, name = result.result()
            associations[name] = dbs
            pbar.update()
    return associations


def get_diss_blocks_wrapper(kwargs):
    return get_diss_blocks(**kwargs)


def get_diss_blocks(PolDiv, Polygons_diss_block):
    dbs_in_pol_div = []
    polygon_PolDiv = build_shape(PolDiv)
    bbox_pd = list(PolDiv.get("shape").bbox)
    for dbuid, (polygon_db, bbox_db) in Polygons_diss_block.items():
        if do_bounding_boxes_overlap(bbox_pd, bbox_db) or do_bounding_boxes_overlap(bbox_db, bbox_pd):
            if do_polygons_overlap(polygon_PolDiv, polygon_db, intersection_pct_over_p2=0.9):
                dbs_in_pol_div.append(dbuid)
            elif do_polygons_overlap(polygon_db, polygon_PolDiv, minimum_area=0.0, intersection_pct_over_p2=0.9):
                dbs_in_pol_div.append(dbuid)
                if PolDiv.get('pd_type') == "S":
                    return dbs_in_pol_div, get_pol_div_str(PolDiv)
    return dbs_in_pol_div, get_pol_div_str(PolDiv)


def get_pol_div_str(PolDiv):
    if PolDiv.get('pd_type') == 'M':
        return f"{PolDiv.get('fed_num')}-{PolDiv.get('pd_num')}-{PolDiv.get('pd_nbr_sfx', 0)}-" \
               f"{PolDiv.get('poll_name').replace('/', '').replace('-', '').replace(' ', '')}-" \
               f"{PolDiv.get('bldg_namee').replace('-', '').replace(' ', '')}"
    return f"{PolDiv.get('fed_num')}-{PolDiv.get('pd_num')}-{PolDiv.get('pd_nbr_sfx', 0)}"


def write_association_file(results, outfile=f"./output/PolDiv_DB_association{time_for_filename()}.json"):
    print(f"writing file {outfile}")
    test_directory(outfile)
    with open(outfile, "w", encoding="utf-8") as output:
        json.dump(results, output, indent=4)


def do_polygons_overlap(polygon1, polygon2, minimum_area=1.0, intersection_pct_over_p2=None):
    """
    Returns True if polygons intersect.
    minimum_area required for intersection to have to be true (absolute value)
    """
    try:
        if not polygon1.intersects(polygon2):
            return False
        intersection = polygon1.intersection(polygon2)
        if intersection.area <= 0.0:
            return False
        if intersection.area <= minimum_area:
            return False
        if intersection_pct_over_p2:
            if intersection.area / polygon2.area <= intersection_pct_over_p2:
                return False
    except TopologicalError:
        print(f"{minimum_area} \t {intersection_pct_over_p2}")
        # plt.plot(*polygon1.exterior.xy, color="Blue", marker='o')
        # plt.plot(*polygon2.exterior.xy, color="Orange", marker='^')
        # plt.show()
        return False
    return True


def do_bounding_boxes_overlap(bbox1, bbox2):
    """Checks to see if two shapefile bboxes are overlapping.
    If you ain't using the shapefile, the boxes should be in the form of
    left, down, right, up"""
    left1, down1, right1, up1 = list(bbox1)
    left2, down2, right2, up2 = list(bbox2)
    return not (right1 <= left2 or up1 <= down2 or left1 >= right2 or down1 >= up2)


def build_shape(shape, allow_holes=True):
    if not isinstance(shape, shapefile.Shape):
        shape = shape["shape"]
    parts = list(shape.parts) + [-1]
    holes = [Polygon(list(shape.points)[i:j]) for i, j in zip(parts, parts[1:])]
    polygon = holes.pop(0)
    if allow_holes:
        holes = holes if len(holes) > 0 else None
    else:
        holes = None
    polygon = Polygon(polygon, holes=holes)
    while not polygon.is_valid:
        # plt.plot(*polygon.exterior.xy, color="Blue", marker='o')
        # if holes:
        #     for interior in polygon.interiors:
        #         plt.plot(*interior.exterior.xy, color="Orange", marker='^')
        # plt.show()
        # plt.close()
        polygon = polygon.buffer(-1)
        # print("Invalid Polygon!")
        # plt.plot(*polygon.exterior.xy, color="Orange", marker='*')
        # if holes:
        #     for interior in polygon.interiors:
        #         plt.plot(*interior.exterior.xy, color="Purple", marker='V')
        # plt.show()
        # plt.close()
    return polygon


if __name__ == '__main__':
    pr_ids = [11, 10, 12, 13,
              24, 35,
              46, 48,
              59,
              60, 61, 62]
    for pr in pr_ids:
        associate_poldiv_and_dbs(prov_id=pr, multiprocess=False)
