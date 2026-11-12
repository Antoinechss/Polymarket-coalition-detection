from river import anomaly, compose, preprocessing

def HalfSpaceTree(): 
    return compose.Pipeline(
        preprocessing.MinMaxScaler(),
        anomaly.HalfSpaceTrees(n_trees=25, height=6, window_size=1000, seed=42))

