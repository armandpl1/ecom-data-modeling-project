"""Generator for the raw e-commerce source data (CDC-style extracts).

Modules, in the order to read them:

    config         reference data: catalog, plans, segments, CSV columns
    history        the CDC mechanics shared by every table: writing a version
                   of a record, nightly load times, late arrivals,
                   corrections, duplicates
    products       product catalog and price history
    customers      customer profiles and their changes
    orders         orders and their status lifecycle
    subscriptions  subscriptions and their month-by-month changes
    writer         CSV formatting and output

The command-line entry point is scripts/generate_raw_data.py.
"""
