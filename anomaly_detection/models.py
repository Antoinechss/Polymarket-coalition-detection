from river import anomaly, compose, preprocessing

def HalfSpaceTree(): 
    return compose.Pipeline(
        preprocessing.MinMaxScaler(),
        anomaly.HalfSpaceTrees(n_trees=25, height=8, window_size=250, seed=42))

