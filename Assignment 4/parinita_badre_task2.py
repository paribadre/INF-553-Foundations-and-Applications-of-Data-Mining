from collections import defaultdict
import copy
import pyspark
import sys


def bfs_and_credits(n_e_dict, root):
    visited = [root]
    parent = defaultdict(set)
    tree_level_dict = defaultdict(set)
    parents_value = defaultdict(float)
    weight = defaultdict(float)
    adjacent_root = n_e_dict[root]
    parent[root] = None
    parents_value[root] = 1
    tree_level_dict[root] = 0
    for current_node in adjacent_root:
        visited.append(current_node)
        parents_value[current_node] = 1
        parent[current_node] = [root]
        tree_level_dict[current_node] = 1
    i = 1
    while i <= len(visited) - 1:
        node = visited[i]
        weight[node] = 1
        adjacent_nodes = n_e_dict[node]
        value_i = 0
        for current_node in parent[node]:
            value_i += parents_value[current_node]
        parents_value[node] = value_i
        for current_node in adjacent_nodes:
            if current_node not in visited:
                visited.append(current_node)
                tree_level_dict[current_node] = tree_level_dict[node] + 1
                parent[current_node] = [node]
            else:
                if tree_level_dict[current_node] == tree_level_dict[node] + 1:
                    parent[current_node].append(node)
        i += 1
    visited_r = list(reversed(visited))
    for a in visited_r[:-1]:
        for b in parent[a]:
            edge = tuple(sorted([a, b]))
            edge_betweenness = weight[a] * (parents_value[b] / parents_value[a])
            weight[b] += edge_betweenness
            yield (edge, edge_betweenness)


def generate_communities(vertices_list):
    all_nodes_visited = []
    communities = []
    for root in vertices_list:
        if root not in all_nodes_visited:
            visited = [root]
            i = 0
            while i <= len(visited) - 1:
                node = visited[i]
                adjacent_nodes = node_edge_dict[node]
                for current_node in adjacent_nodes:
                    if current_node not in visited:
                        visited.append(current_node)
                i += 1
            visited.sort()
            all_nodes_visited += visited
            communities.append(visited)
    return communities


if __name__ == '__main__':

    input_file_path = sys.argv[1]
    betweenness_output_file_path = sys.argv[2]
    community_output_file_path = sys.argv[3]

    sc = pyspark.SparkContext('local[*]', 'Task2')
    rdd = sc.textFile(input_file_path)

    # Create a dictionary with node and corresponding connected nodes
    node_edge_dict = rdd.map(lambda x: x.split(" ")).flatMap(lambda x: [(x[0], [x[1]]), (x[1], [x[0]])]).reduceByKey(lambda x, y: x+y).collectAsMap()
    # Create a list of all vertices
    vertices_list = list(node_edge_dict.keys())

    # Task 2.1: Betweenness Calculation and write to file
    betweenness = sc.parallelize(vertices_list).flatMap(lambda x: bfs_and_credits(node_edge_dict, x))\
        .reduceByKey(lambda x, y: x+y).map(lambda x: (x[0], x[1]/2))\
        .sortBy(lambda x: (-x[1], x[0])).collect()

    with open(betweenness_output_file_path, "w") as fout:
        for i in betweenness:
            fout.write("(\'{}\', \'{}\'), {}\n".format(i[0][0], i[0][1], i[1]))
        fout.close()

    # Task 2.2: Community Detection
    m = len(betweenness)
    original_node_edge_dict = copy.deepcopy(node_edge_dict)
    best_modularity = 0
    updated_betweenness = betweenness
    best_community = []
    no_of_cuts = 0

    while no_of_cuts < m:
        current_modularity = 0
        communities = generate_communities(vertices_list)
        for current_community in communities:
            total_q = 0
            for i in current_community:
                for j in current_community:
                    if i != j:
                        k_i = len(original_node_edge_dict[i])
                        k_j = len(original_node_edge_dict[j])
                        if j in original_node_edge_dict[i]:
                            a_ij = 1
                        else:
                            a_ij = 0
                        q = (a_ij - (k_i * k_j / (2 * m))) / (2 * m)
                        total_q += q
            current_modularity += total_q
        if current_modularity > best_modularity:
            best_modularity = current_modularity
            best_community = copy.deepcopy(communities)

        updated_betweenness = sc.parallelize(vertices_list).flatMap(lambda x: bfs_and_credits(node_edge_dict, x)).reduceByKey(lambda x, y: x + y) \
            .map(lambda x: (x[1]/2, [x[0]])).reduceByKey(lambda x, y: x+y).sortBy(lambda x: (-x[0])).first()
        edges_cut = updated_betweenness[1]
        for current_edge in edges_cut:
            node_1 = current_edge[0]
            node_2 = current_edge[1]
            node_edge_dict[node_1].remove(node_2)
            node_edge_dict[node_2].remove(node_1)
        no_of_cuts += len(edges_cut)

    best_modularity_community = sc.parallelize(best_community).sortBy(lambda x: (len(x), x)).collect()

    with open(community_output_file_path, 'w') as fout:
        for i in best_modularity_community:
            fout.write(str(i).strip('[').strip(']') + '\n')
        fout.close()
