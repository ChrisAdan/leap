import numpy as np

def step(x, y, z, speed, t):
    """
    Compute the next position using Lissajous curve dynamics.

    Produces harmonic motion in three dimensions with independent frequencies and phases.

    Args:
        x (float): Current x-coordinate.
        y (float): Current y-coordinate.
        z (float): Current z-coordinate.
        speed (float): Player movement speed.
        t (int): Timestep index.

    Returns:
        tuple[float, float, float]: Updated (x, y, z) position.
    """
    dx = np.sin(3 * t * 0.05) * speed * 0.5
    dy = np.sin(4 * t * 0.05 + np.pi / 2) * speed * 0.5
    dz = np.sin(5 * t * 0.05 + np.pi) * speed * 0.5
    return x + dx, y + dy, z + dz
