import numpy as np

def step(x, y, z, speed, t):
    """
    Compute the next position using Lorentzian-style motion.

    Movement accelerates near the origin and slows down as position increases,
    mimicking a Lorentzian curve along the x-axis. Y and Z follow sinusoidal oscillations.

    Args:
        x (float): Current x-coordinate.
        y (float): Current y-coordinate.
        z (float): Current z-coordinate.
        speed (float): Player movement speed.
        t (int): Timestep index.

    Returns:
        tuple[float, float, float]: Updated (x, y, z) position.
    """
    dx = speed / (1 + x ** 2)
    dy = np.sin(t / 10.0) * speed * 0.1
    dz = np.cos(t / 10.0) * speed * 0.1
    return x + dx, y + dy, z + dz
