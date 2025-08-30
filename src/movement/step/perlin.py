from noise import pnoise1 # type: ignore

def step(x, y, z, speed, t):
    """
    Compute the next position using Perlin noise for smooth pseudo-random movement.

    Uses 1D Perlin noise functions with time-based offsets to generate correlated motion.

    Args:
        x (float): Current x-coordinate.
        y (float): Current y-coordinate.
        z (float): Current z-coordinate.
        speed (float): Player movement speed.
        t (int): Timestep index.

    Returns:
        tuple[float, float, float]: Updated (x, y, z) position.
    """
    dx = pnoise1(t * 0.1) * speed
    dy = pnoise1((t + 100) * 0.1) * speed
    dz = pnoise1((t + 200) * 0.1) * speed
    return x + dx, y + dy, z + dz
