def convert_mature_h5_to_sequential(position_aa: int) -> int:
    # mapping based on https://github.com/dms-vep/Flu_H5_American-Wigeon_South-Carolina_2021-H5N1_DMS/raw/refs/heads/main/data/site_numbering_map.csv
    if -16 <= position_aa <= -1:
        return position_aa + 17
    elif 1 <= position_aa <= 552:
        return position_aa + 16
    else:
        raise ValueError(f'{position_aa} is not recognized as a mature H5 site.')

