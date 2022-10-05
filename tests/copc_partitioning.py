# %%
from laspy import CopcReader, Bounds
import random
import math
import numpy as np


random.seed(42)

# path = "http://192.168.1.110:9000/geospatial/copc/CA_YosemiteNP_2019/USGS_LPC_CA_YosemiteNP_2019_D19_11SKB6892.laz"
path = "/home/aitor-pc/Projects/cloud-data/cloud-native-datasets/sample_data/copc/CA_YosemiteNP_2019/USGS_LPC_CA_YosemiteNP_2019_D19_11SKB6892.laz"

# %%
with CopcReader.open(path) as crdr:
    total_points = crdr.header.point_count
    print(f'Total points: {total_points}')
    partitions = 9
    partition_size = math.ceil(total_points / partitions)
    print(f'Ideal partition size: {partition_size}')

    rp = crdr.root_page
    nodes = {}

    for voxel_key, voxel_entry in crdr.root_page.entries.items():
        if voxel_key.level in nodes:
            nodes[voxel_key.level].append(voxel_entry)
        else:
            nodes[voxel_key.level] = [voxel_entry]

    for level, entries in nodes.items():
        point_count = sum([entry.point_count for entry in entries])
        print(
            f'Level {level} - no. voxels {len(entries)} - point count is {point_count} - {round((point_count * 100) / total_points, 2)}% of total')

    voxels_per_partition = len(crdr.root_page.entries) / partitions
    print(f'Aprox. {voxels_per_partition} voxels will be added per partition')

    print('Calculating centroinds...')
    centroids = []
    for voxel_key, voxel_entry in crdr.root_page.entries.items():
        points = crdr._fetch_and_decrompress_points_of_nodes([voxel_entry])
        centroid = np.array([np.mean(points.x), np.mean(points.y), np.mean(points.z)])
        centroids.append((voxel_key, centroid))
        # print(centroid)
    print('Done')

    # %%
    partitions = []

    while (centroids):
        current_index = random.randint(0, len(centroids) - 1)
        current = centroids.pop(current_index)
        distances = [(i, (p[0], np.linalg.norm(current[1] - p[1]))) for i, p in enumerate(centroids)]
        distances.sort(key=lambda tup: tup[1][1])

        partition = {
            'points': 0,
            'voxels': []
        }

        indexes_to_delete = []
        for centroid_index, distance_tup in distances:
            voxel_key, distance = distance_tup
            voxel = crdr.root_page.entries[voxel_key]

            partition['points'] += voxel.point_count
            partition['voxels'].append(voxel)
            indexes_to_delete.append(centroid_index)

            if partition['points'] >= partition_size:
                partitions.append(partition)
                print(f"Created partition with {len(partition['voxels'])} voxels and and {partition['points']} points")
                break

        print(len(centroids))
        for index_to_delete in indexes_to_delete:
            centroids[index_to_delete] = None
        centroids = [c for c in centroids if c is not None]
        print(len(centroids))

    # nodes = list(rp.entries)
    # coords = [(voxel.x, voxel.y, voxel.z) for voxel in nodes]
    # X = np.array(coords)
    # print(X)
    # model = AgglomerativeClustering(linkage="average", affinity="cosine")
    # model.fit(X)
    # pprint(nodes)

# %%
