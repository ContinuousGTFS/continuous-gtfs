"""Seed ctx.output with both upstream RT feeds (identity init)."""

from continuous_gtfs import step


@step(before="*")
def init_outputs(ctx):
    ctx.output["vehicle_positions"] = ctx.inputs["vehicle_positions"]
    ctx.output["trip_updates"] = ctx.inputs["trip_updates"]
