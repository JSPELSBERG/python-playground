from gdpslib import utils
from gdpslib.ifrs import shared as gsh


@utils.registerExtService(isComplex=False)
def shExtensionServiceCohort(*, COHORT, START_DATE_OF_COVERAGE, **_):
    """
    Extend the input data with the column `COHORT_EXT`.

    The field `COHORT_EXT` is derived by either `COHORT`
    or `START_DATE_OF_COVERAGE`.

    Parameters
    ----------
    COHORT : str
        Name of the `COHORT` column, e.g. "COHORT".
        If `COHORT` is filled with a valid value than this
        is used for the `COHORT_EXT` field.
    START_DATE_OF_COVERAGE : str
        Name of the `START_DATE_OF_COVERAGE` column,
        e.g. "START_DATE_OF_COVERAGE".
        If `COHORT` is not a valid year, `COHORT_EXT` is derived
        from this column.
    **_ : dict, optional
        Used to catch additional keyword arguments.

    Returns
    -------
    DataFrame
        Spark DataFrame with additional `COHORT_EXT` column.

    Notes
    -----
    Invalid rows are sent to the error handler.
    """

    def _(dfInput):

        validCond = f"""
            ({COHORT} IS NOT NULL AND LENGTH({COHORT})=4 AND INT({COHORT}) IS NOT NULL)
            OR (({COHORT} IS NULL OR {COHORT}='') AND {START_DATE_OF_COVERAGE} IS NOT NULL)
            """

        dfValidCohort = dfInput.where(validCond)
        dfInvalidCohort = dfInput.where(f"!({validCond})")
        dfCombined = gsh.ext.shErrorHandling(
            dfValid=dfValidCohort, dfErrors=dfInvalidCohort, idError=35, reloadFlag=0
        )

        dfOutput = dfCombined.selectExpr(
            "*",
            f"""
                CASE WHEN ({COHORT} IS NULL OR {COHORT} ='') 
                THEN year({START_DATE_OF_COVERAGE}) 
                ELSE {COHORT} END as COHORT_EXT
            """,
        )
        return dfOutput

    return _


__all__ = ["shExtensionServiceCohort"]
