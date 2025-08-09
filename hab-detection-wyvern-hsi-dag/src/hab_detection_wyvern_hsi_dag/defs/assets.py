import dagster as dg


@dg.asset
def assets(context: dg.AssetExecutionContext) -> dg.MaterializeResult: ...
