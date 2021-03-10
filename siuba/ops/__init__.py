from .generics import ALL_OPS, PLAIN_OPS

# import accessor generics. These are included in ALL_OPS, but since we want
# users to be able to import from them, also need to be modules. Start their
# names with underscores just to keep the files together.

globals().update(PLAIN_OPS)

