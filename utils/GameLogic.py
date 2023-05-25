def gameEnd(vin_score, ka_score):
    if vin_score == 11 and ka_score < 10:
        return (True, "Vineesh")
    elif ka_score == 11 and vin_score < 10:
        return (True, "Kaleb")
    elif vin_score >= 10 and ka_score >= 10:
        if (ka_score - vin_score) == 2:
            return (True, "Kaleb")
        elif (vin_score - ka_score) == 2:
            return (True, "Vineesh")
        else:
            return (False, "NA")
    else:
        return (False, "NA")