def get_english_name(species):
    return species[:species.index("(")];

def get_start_year(period):

    if "-" in period:
        return int(period[:period.index("-")].replace("(","").replace(")","").strip());
    else:
        return int(period.replace("(","").replace(")","").strip());


def get_trend(annual_percentage_change):
    if (annual_percentage_change <= -3.00):
        return "strong decline";
    elif (annual_percentage_change > -3.00 and annual_percentage_change <= -0.50):
        return "weak decline";
    elif (annual_percentage_change > -0.50 and annual_percentage_change <= 0.50):
        return "no change";
    elif (annual_percentage_change > 0.50 and annual_percentage_change <= 3.00):
        return "weak increase";
    elif (annual_percentage_change > 3.00):
        return "strong increase";