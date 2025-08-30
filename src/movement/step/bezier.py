import numpy as np

def step(x, y, z, speed, t):
    """
    Compute the next position along a simple quadratic Bezier curve.

    This uses static control points and blends the player's position toward
    the Bezier path using time as the curve parameter.

    Args:
        x (float): Current x-coordinate.
        y (float): Current y-coordinate.
        z (float): Current z-coordinate.
        speed (float): Player movement speed.
        t (int): Timestep index.

    Returns:
        tuple[float, float, float]: Updated (x, y, z) position.
    """
    control = np.array([[20, 30, 40], [-40, -30, -20], [0, 0, 0]])
    weights = np.array([(1 - t % 1) ** 2, 2 * (1 - t % 1) * (t % 1), (t % 1) ** 2])
    bx, by, bz = (weights @ control).tolist()

    dx = (bx - x) * speed * 0.05
    dy = (by - y) * speed * 0.05
    dz = (bz - z) * speed * 0.05
    return x + dx, y + dy, z + dz
