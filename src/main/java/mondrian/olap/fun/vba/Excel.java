/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2008-2009 Pentaho
// All Rights Reserved.
*/
package mondrian.olap.fun.vba;

import mondrian.olap.InvalidArgumentException;
import mondrian.olap.fun.JavaFunDef;

import static mondrian.olap.fun.JavaFunDef.Description;
import static mondrian.olap.fun.JavaFunDef.FunctionName;

/**
 * Implementations of functions in the Excel worksheet library.
 *
 * <p>Functions are loaded into the function table by reflection.
 *
 * @author jhyde
 * @since Dec 31, 2007
 */
public abstract class Excel {
    // There follows a list of all functions defined in Excel. Functions are
    // marked 'todo:' if they still need to be implemented; 'skip:' if they
    // are implemented elsewhere, such as in Vba or there there is an explicit
    // implementation of FunDef for them. A FunDef typically allows a more
    // efficient implementation.

    //   AccrInt Returns the accrued interest for a security that pays periodic
    //   interest.

    //  AccrIntM Returns the accrued interest for a security that pays interest
    //  at maturity.

    @FunctionName("Acos")
    @JavaFunDef.Signature("Acos(number)")
    @Description(
        "Returns the arccosine, or inverse cosine, of a number. The arccosine "
        + "is the angle whose cosine is Arg1. The returned angle is given in "
        + "radians in the range 0 (zero) to pi.")
    public static double acos(double number) {
        return Math.acos(number);
    }

    @FunctionName("Acosh")
    @JavaFunDef.Signature("Acosh(number)")
    @Description(
        "Returns the inverse hyperbolic cosine of a number. Number must be "
        + "greater than or equal to 1. The inverse hyperbolic cosine is the "
        + "value whose hyperbolic cosine is Arg1, so Acosh(Cosh(number)) "
        + "equals Arg1.")
    public static double acosh(double x) {
        return Math.log(x + Math.sqrt((x * x) - 1.0));
    }

    // Todo: AmorDegrc Returns the depreciation for each accounting
    // period. This function is provided for the French accounting
    // system.

    // Todo: AmorLinc Returns the depreciation for each accounting
    // period. This function is provided for the French accounting
    // system.

    // Skip: And Returns TRUE if all its arguments are TRUE; returns
    // FALSE if one or more argument is FALSE.

    // Todo: Asc For Double-byte character set (DBCS) languages,
    // changes full-width (double-byte) characters to half-width
    // (single-byte) characters.

    @FunctionName("Asin")
    @JavaFunDef.Signature("Asin(number)")
    @Description(
        "Returns the arcsine, or inverse sine, of a number. The arcsine is the "
        + "angle whose sine is Arg1. The returned angle is given in radians in "
        + "the range -pi/2 to pi/2.")
    public static double asin(double number) {
        return Math.asin(number);
    }

    @FunctionName("Asinh")
    @JavaFunDef.Signature("Asinh(number)")
    @Description(
        "Returns the inverse hyperbolic sine of a number. The inverse "
        + "hyperbolic sine is the value whose hyperbolic sine is Arg1, "
        + "so Asinh(Sinh(number)) equals Arg1.")
    public static double asinh(double x) {
        return Math.log(x + Math.sqrt(1.0 + (x * x)));
    }

    @FunctionName("Atan2")
    @JavaFunDef.Signature("Atan2(x, y)")
    @Description(
        "Returns the arctangent, or inverse tangent, of the specified x- and "
        + "y-coordinates. The arctangent is the angle from the x-axis to a "
        + "line containing the origin (0, 0) and a point with coordinates "
        + "(x_num, y_num). The angle is given in radians between -pi and pi, "
        + "excluding -pi.")
    public static double atan2(double y, double x) {
        return Math.atan2(y, x);
    }

    @FunctionName("Atanh")
    @JavaFunDef.Signature("Atanh(number)")
    @Description(
        "Returns the inverse hyperbolic tangent of a number. Number "
        + "must be between -1 and 1 (excluding -1 and 1).")
    public static double atanh(double x) {
        return .5 * Math.log((1.0 + x) / (1.0 - x));
    }

    // Todo: AveDev Returns the average of the absolute deviations of data
    // points from their mean. AveDev is a measure of the variability in a data
    // set.

    // Todo: Average Returns the average (arithmetic mean) of the arguments.

    // Todo: AverageIf Returns the average (arithmetic mean) of all the cells in
    // a range that meet a given criteria.

    // Todo: AverageIfs Returns the average (arithmetic mean) of all cells that
    // meet multiple criteria.

    // Todo: BahtText Converts a number to Thai text and adds a suffix of
    // "Baht."

    // Todo: BesselI Returns the modified Bessel function, which is equivalent
    // to the Bessel function evaluated for purely imaginary arguments.

    // Todo: BesselJ   Returns the Bessel function.

    // Todo: BesselK Returns the modified Bessel function, which is equivalent
    // to the Bessel functions evaluated for purely imaginary arguments.

    // Todo: BesselY Returns the Bessel function, which is also called the Weber
    // function or the Neumann function.

    // Todo: BetaDist   Returns the beta cumulative distribution function.

    // Todo: BetaInv Returns the inverse of the cumulative distribution function
    // for a specified beta distribution. That is, if probability =
    // BetaDist(x,...), then BetaInv(probability,...) = x.

    // Todo: Bin2Dec   Converts a binary number to decimal.

    // Todo: Bin2Hex   Converts a binary number to hexadecimal.

    // Todo: Bin2Oct   Converts a binary number to octal.

    // Todo: BinomDist Returns the individual term binomial distribution
    // probability.

    // Todo: Ceiling Returns number rounded up, away from zero, to the nearest
    // multiple of significance.

    // Todo: ChiDist Returns the one-tailed probability of the chi-squared
    // distribution.

    // Todo: ChiInv Returns the inverse of the one-tailed probability of the
    // chi-squared distribution.

    // Todo: ChiTest   Returns the test for independence.

    // Todo: Choose Uses Arg1 as the index to return a value from the list of
    // value arguments.

    // Todo: Clean   Removes all nonprintable characters from text.

    // Todo: Combin Returns the number of combinations for a given number of
    // items. Use Combin to determine the total possible number of groups for a
    // given number of items.

    // Todo: Complex Converts real and imaginary coefficients into a complex
    // number of the form x + yi or x + yj.

    // Todo: Confidence Returns a value that you can use to construct a
    // confidence interval for a population mean.

    // Todo: Convert Converts a number from one measurement system to
    // another. For example, Convert can translate a table of distances in miles
    // to a table of distances in kilometers.

    // Todo: Correl Returns the correlation coefficient of the Arg1 and Arg2
    // cell ranges.

    @FunctionName("Cosh")
    @Description("Returns the hyperbolic cosine of a number.")
    public static double cosh(double number) {
        return Math.cosh(number);
    }

    // Todo: Count Counts the number of cells that contain numbers and counts
    // numbers within the list of arguments.

    // Todo: CountA Counts the number of cells that are not empty and the values
    // within the list of arguments.

    // Todo: CountBlank Counts empty cells in a specified range of cells.

    // Todo: CountIf Counts the number of cells within a range that meet the
    // given criteria.

    // Todo: CountIfs Counts the number of cells within a range that meet
    // multiple criteria.

    // Todo: CoupDayBs Returns the number of days from the beginning of the
    // coupon period to the settlement date.

    // Todo: CoupDays Returns the number of days in the coupon period that
    // contains the settlement date.

    // Todo: CoupDaysNc Returns the number of days from the settlement date to
    // the next coupon date.

    // Todo: CoupNcd Returns a number that represents the next coupon date after
    // the settlement date.

    // Todo: CoupNum Returns the number of coupons payable between the
    // settlement date and maturity date, rounded up to the nearest whole
    // coupon.

    // Todo: CoupPcd The description for this item will appear in the final
    // release of Office 2007.

    // Todo: Covar Returns covariance, the average of the products of deviations
    // for each data point pair.

    // Todo: CritBinom Returns the smallest value for which the cumulative
    // binomial distribution is greater than or equal to a criterion value.

    // Todo: CumIPmt Returns the cumulative interest paid on a loan between
    // start_period and end_period.

    // Todo: CumPrinc Returns the cumulative principal paid on a loan between
    // start_period and end_period.

    // Todo: DAverage Averages the values in a column of a list or database that
    // match conditions you specify.

    // Todo: Days360 Returns the number of days between two dates based on a
    // 360-day year (twelve 30-day months), which is used in some accounting
    // calculations.

    // Todo: Db Returns the depreciation of an asset for a specified period
    // using the fixed-declining balance method.

    // Todo: Dbcs The description for this item will appear in the final release
    // of Office 2007.

    // Todo: DCount Counts the cells that contain numbers in a column of a list
    // or database that match conditions that you specify.

    // Todo: DCountA Counts the nonblank cells in a column of a list or database
    // that match conditions that you specify.

    // Todo: Ddb Returns the depreciation of an asset for a specified period
    // using the double-declining balance method or some other method you
    // specify.

    // Todo: Dec2Bin Converts a decimal number to binary.

    // Todo: Dec2Hex Converts a decimal number to hexadecimal.

    // Todo: Dec2Oct Converts a decimal number to octal.

    // Todo: Degrees Converts radians into degrees.


    @FunctionName("Degrees")
    @Description("Converts radians to degrees.")
    public static double degrees(double number) {
        // 180 degrees = Pi radians
        return number * 180.0 / Math.PI;
    }

    // Todo: Delta Tests whether two values are equal. Returns 1 if number1 =
    // number2; returns 0 otherwise.

    // Todo: DevSq Returns the sum of squares of deviations of data points from
    // their sample mean.

    // Todo: DGet Extracts a single value from a column of a list or database
    // that matches conditions that you specify.

    // Todo: Disc Returns the discount rate for a security.

    // Todo: DMax Returns the largest number in a column of a list or database
    // that matches conditions you that specify.

    // Todo: DMin Returns the smallest number in a column of a list or database
    // that matches conditions that you specify.

    // Todo: Dollar The function described in this Help topic converts a number
    // to text format and applies a currency symbol. The name of the function
    // (and the symbol that it applies) depends upon your language settings.

    // Todo: DollarDe Converts a dollar price expressed as a fraction into a
    // dollar price expressed as a decimal number. Use DOLLARDE to convert
    // fractional dollar numbers, such as securities prices, to decimal numbers.

    // Todo: DollarFr Converts a dollar price expressed as a decimal number into
    // a dollar price expressed as a fraction. Use DOLLARFR to convert decimal
    // numbers to fractional dollar numbers, such as securities prices.

    // Todo: DProduct Multiplies the values in a column of a list or database
    // that match conditions that you specify.

    // Todo: DStDev Estimates the standard deviation of a population based on a
    // sample by using the numbers in a column of a list or database that match
    // conditions that you specify.

    // Todo: DStDevP Calculates the standard deviation of a population based on
    // the entire population by using the numbers in a column of a list or
    // database that match conditions that you specify.

    // Todo: DSum Adds the numbers in a column of a list or database that match
    // conditions that you specify.

    // Todo: Duration Returns the Macauley duration for an assumed par value of
    // $100. Duration is defined as the weighted average of the present value of
    // the cash flows and is used as a measure of a bond price's response to
    // changes in yield.

    // Todo: DVar Estimates the variance of a population based on a sample by
    // using the numbers in a column of a list or database that match conditions
    // that you specify.

    // Todo: DVarP Calculates the variance of a population based on the entire
    // population by using the numbers in a column of a list or database that
    // match conditions that you specify.

    // Todo: EDate Returns the serial number that represents the date that is
    // the indicated number of months before or after a specified date (the
    // start_date). Use EDATE to calculate maturity dates or due dates that fall
    // on the same day of the month as the date of issue.

    // Todo: Effect Returns the effective annual interest rate, given the
    // nominal annual interest rate and the number of compounding periods per
    // year.

    // Todo: EoMonth Returns the serial number for the last day of the month
    // that is the indicated number of months before or after start_date. Use
    // EOMONTH to calculate maturity dates or due dates that fall on the last
    // day of the month.

    // Todo: Erf Returns the error function integrated between lower_limit and
    // upper_limit.

    // Todo: ErfC The description for this item will appear in the final release
    // of Office 2007.

    // Todo: Even Returns number rounded up to the nearest even integer. You can
    // use this function for processing items that come in twos. For example, a
    // packing crate accepts rows of one or two items. The crate is full when
    // the number of items, rounded up to the nearest two, matches the crate's
    // capacity.

    // Todo: ExponDist Returns the exponential distribution. Use EXPONDIST to
    // model the time between events, such as how long an automated bank teller
    // takes to deliver cash. For example, you can use EXPONDIST to determine
    // the probability that the process takes at most 1 minute.

    // Todo: Fact Returns the factorial of a number. The factorial of a number
    // is equal to 1*2*3*...* number.

    // Todo: FactDouble Returns the double factorial of a number.

    // Todo: FDist Returns the F probability distribution. You can use this
    // function to determine whether two data sets have different degrees of
    // diversity. For example, you can examine the test scores of men and women
    // entering high school and determine if the variability in the females is
    // different from that found in the males.

    // Todo: Find Finds specific information in a worksheet.

    // Todo: FindB FIND and FINDB locate one text string within a second text
    // string, and return the number of the starting position of the first text
    // string from the first character of the second text string.

    // Todo: FInv Returns the inverse of the F probability distribution. If p =
    // FDIST(x,...), then FINV(p,...) = x.

    // Todo: Fisher Returns the Fisher transformation at x. This transformation
    // produces a function that is normally distributed rather than skewed. Use
    // this function to perform hypothesis testing on the correlation
    // coefficient.

    // Todo: FisherInv Returns the inverse of the Fisher transformation. Use
    // this transformation when analyzing correlations between ranges or arrays
    // of data. If y = FISHER(x), then FISHERINV(y) = x.

    // Todo: Fixed Rounds a number to the specified number of decimals, formats
    // the number in decimal format using a period and commas, and returns the
    // result as text.

    // Todo: Floor Rounds number down, toward zero, to the nearest multiple of
    // significance.

    // Todo: Forecast Calculates, or predicts, a future value by using existing
    // values. The predicted value is a y-value for a given x-value. The known
    // values are existing x-values and y-values, and the new value is predicted
    // by using linear regression. You can use this function to predict future
    // sales, inventory requirements, or consumer trends.

    // Todo: Frequency Calculates how often values occur within a range of
    // values, and then returns a vertical array of numbers. For example, use
    // FREQUENCY to count the number of test scores that fall within ranges of
    // scores. Because FREQUENCY returns an array, it must be entered as an
    // array formula.

    // Todo: FTest Returns the result of an F-test. An F-test returns the
    // two-tailed probability that the variances in array1 and array2 are not
    // significantly different. Use this function to determine whether two
    // samples have different variances. For example, given test scores from
    // public and private schools, you can test whether these schools have
    // different levels of test score diversity.

    // Todo: Fv Returns the future value of an investment based on periodic,
    // constant payments and a constant interest rate.

    // Todo: FVSchedule Returns the future value of an initial principal after
    // applying a series of compound interest rates. Use FVSCHEDULE to calculate
    // the future value of an investment with a variable or adjustable rate.

    // Todo: GammaDist Returns the gamma distribution. You can use this function
    // to study variables that may have a skewed distribution. The gamma
    // distribution is commonly used in queuing analysis.

    // Todo: GammaInv Returns the inverse of the gamma cumulative
    // distribution. If p = GAMMADIST(x,...), then GAMMAINV(p,...) = x.

    // Todo: GammaLn Returns the natural logarithm of the gamma function, ?(x).

    // Todo: Gcd Returns the greatest common divisor of two or more
    // integers. The greatest common divisor is the largest integer that divides
    // both number1 and number2 without a remainder.

    // Todo: GeoMean Returns the geometric mean of an array or range of positive
    // data. For example, you can use GEOMEAN to calculate average growth rate
    // given compound interest with variable rates.

    // Todo: GeStep Returns 1 if number ? step; returns 0 (zero) otherwise. Use
    // this function to filter a set of values. For example, by summing several
    // GESTEP functions you calculate the count of values that exceed a
    // threshold.

    // Todo: Growth Calculates predicted exponential growth by using existing
    // data. GROWTH returns the y-values for a series of new x-values that you
    // specify by using existing x-values and y-values. You can also use the
    // GROWTH worksheet function to fit an exponential curve to existing
    // x-values and y-values.

    // Todo: HarMean Returns the harmonic mean of a data set. The harmonic mean
    // is the reciprocal of the arithmetic mean of reciprocals.

    // Todo: Hex2Bin Converts a hexadecimal number to binary.

    // Todo: Hex2Dec Converts a hexadecimal number to decimal.

    // Todo: Hex2Oct Converts a hexadecimal number to octal.

    // Todo: HLookup Searches for a value in the top row of a table or an array
    // of values, and then returns a value in the same column from a row you
    // specify in the table or array. Use HLOOKUP when your comparison values
    // are located in a row across the top of a table of data, and you want to
    // look down a specified number of rows. Use VLOOKUP when your comparison
    // values are located in a column to the left of the data you want to find.

    // Todo: HypGeomDist Returns the hypergeometric distribution. HYPGEOMDIST
    // returns the probability of a given number of sample successes, given the
    // sample size, population successes, and population size. Use HYPGEOMDIST
    // for problems with a finite population, where each observation is either a
    // success or a failure, and where each subset of a given size is chosen
    // with equal likelihood.

    // Todo: IfError Returns a value you specify if a formula evaluates to an
    // error; otherwise, returns the result of the formula. Use the IFERROR
    // function to trap and handle errors in a formula.

    // Todo: ImAbs Returns the absolute value (modulus) of a complex number in x
    // + yi or x + yj text format.

    // Todo: Imaginary Returns the imaginary coefficient of a complex number in
    // x + yi or x + yj text format.

    // Todo: ImArgument Returns the argument (theta), an angle expressed in
    // radians, such that:

    // Todo: ImConjugate Returns the complex conjugate of a complex number in x
    // + yi or x + yj text format.

    // Todo: ImCos Returns the cosine of a complex number in x + yi or x + yj
    // text format.

    // Todo: ImDiv Returns the quotient of two complex numbers in x + yi or x +
    // yj text format.

    // Todo: ImExp Returns the exponential of a complex number in x + yi or x +
    // yj text format.

    // Todo: ImLn Returns the natural logarithm of a complex number in x + yi or
    // x + yj text format.

    // Todo: ImLog10 Returns the common logarithm (base 10) of a complex number
    // in x + yi or x + yj text format.

    // Todo: ImLog2 Returns the base-2 logarithm of a complex number in x + yi
    // or x + yj text format.

    // Todo: ImPower Returns a complex number in x + yi or x + yj text format
    // raised to a power.

    // Todo: ImProduct Returns the product of 2 to 29 complex numbers in x + yi
    // or x + yj text format.

    // Todo: ImReal Returns the real coefficient of a complex number in x + yi
    // or x + yj text format.

    // Todo: ImSin Returns the sine of a complex number in x + yi or x + yj text
    // format.

    // Todo: ImSqrt Returns the square root of a complex number in x + yi or x +
    // yj text format.

    // Todo: ImSub Returns the difference of two complex numbers in x + yi or x
    // + yj text format.

    // Todo: ImSum Returns the sum of two or more complex numbers in x + yi or x
    // + yj text format.

    // Todo: Index Returns a value or the reference to a value from within a
    // table or range. There are two forms of the INDEX function: the array form
    // and the reference form.

    // Todo: Intercept Calculates the point at which a line will intersect the
    // y-axis by using existing x-values and y-values. The intercept point is
    // based on a best-fit regression line plotted through the known x-values
    // and known y-values. Use the INTERCEPT function when you want to determine
    // the value of the dependent variable when the independent variable is 0
    // (zero). For example, you can use the INTERCEPT function to predict a
    // metal's electrical resistance at 0C when your data points were taken at
    // room temperature and higher.

    // Todo: IntRate Returns the interest rate for a fully invested security.

    // Todo: Ipmt Returns the interest payment for a given period for an
    // investment based on periodic, constant payments and a constant interest
    // rate.

    // Todo: Irr Returns the internal rate of return for a series of cash flows
    // represented by the numbers in values. These cash flows do not have to be
    // even, as they would be for an annuity. However, the cash flows must occur
    // at regular intervals, such as monthly or annually. The internal rate of
    // return is the interest rate received for an investment consisting of
    // payments (negative values) and income (positive values) that occur at
    // regular periods.

    // Todo: IsErr Checks the type of value and returns TRUE or FALSE depending
    // if the value refers to any error value except #N/A.

    // Todo: IsError Checks the type of value and returns TRUE or FALSE
    // depending if the value refers to any error value (#N/A, #VALUE!, #REF!,
    // #DIV/0!, #NUM!, #NAME?, or #NULL!).

    // Todo: IsEven Checks the type of value and returns TRUE or FALSE depending
    // if the value is even.

    // Todo: IsLogical Checks the type of value and returns TRUE or FALSE
    // depending if the value refers to a logical value.

    // Todo: IsNA Checks the type of value and returns TRUE or FALSE depending
    // if the value refers to the #N/A (value not available) error value.

    // Todo: IsNonText Checks the type of value and returns TRUE or FALSE
    // depending if the value refers to any item that is not text. (Note that
    // this function returns TRUE if value refers to a blank cell.)

    // Todo: IsNumber Checks the type of value and returns TRUE or FALSE
    // depending if the value refers to a number.

    // Todo: IsOdd Checks the type of value and returns TRUE or FALSE depending
    // if the value is odd.

    // Todo: Ispmt Calculates the interest paid during a specific period of an
    // investment. This function is provided for compatibility with Lotus 1-2-3.

    // Todo: IsText Checks the type of value and returns TRUE or FALSE depending
    // if the value refers to text.

    // Todo: Kurt Returns the kurtosis of a data set. Kurtosis characterizes the
    // relative peakedness or flatness of a distribution compared with the
    // normal distribution. Positive kurtosis indicates a relatively peaked
    // distribution. Negative kurtosis indicates a relatively flat distribution.

    // Todo: Large Returns the k-th largest value in a data set. You can use
    // this function to select a value based on its relative standing. For
    // example, you can use LARGE to return the highest, runner-up, or
    // third-place score.

    // Todo: Lcm Returns the least common multiple of integers. The least common
    // multiple is the smallest positive integer that is a multiple of all
    // integer arguments number1, number2, and so on. Use LCM to add fractions
    // with different denominators.

    // Todo: LinEst Calculates the statistics for a line by using the "least
    // squares" method to calculate a straight line that best fits your data,
    // and returns an array that describes the line. Because this function
    // returns an array of values, it must be entered as an array formula.

    // Todo: Ln Returns the natural logarithm of a number. Natural logarithms
    // are based on the constant e (2.71828182845904).


    // See Vba
    // Skip: Log   Returns the logarithm of a number to the base you specify.

    @FunctionName("Log10")
    @Description("Returns the base-10 logarithm of a number.")
    public static double log10(double number) {
        return Math.log10(number);
    }

    // Todo: LogEst In regression analysis, calculates an exponential curve that
    // fits your data and returns an array of values that describes the
    // curve. Because this function returns an array of values, it must be
    // entered as an array formula.

    // Todo: LogInv Use the lognormal distribution to analyze logarithmically
    // transformed data.

    // Todo: LogNormDist Returns the cumulative lognormal distribution of x,
    // where ln(x) is normally distributed with parameters mean and
    // standard_dev. Use this function to analyze data that has been
    // logarithmically transformed.

    // Todo: Lookup Returns a value either from a one-row or one-column range or
    // from an array. The LOOKUP function has two syntax forms: the vector form
    // and the array form.

    // Todo: Match Returns the relative position of an item in an array that
    // matches a specified value in a specified order. Use MATCH instead of one
    // of the LOOKUP functions when you need the position of an item in a range
    // instead of the item itself.

    // Skip: Max Returns the largest value in a set of values.  Todo: MDeterm
    // Returns the matrix determinant of an array.

    /**
     * The MOD function. Not technically in the Excel package, but this seemed
     * like a good place to put it, since Excel has a MOD function.
     *
     * @param first First
     * @param second Second
     * @return First modulo second
     */
    @FunctionName("Mod")
    @JavaFunDef.Signature("Mod(n, d)")
    @Description("Returns the remainder of dividing n by d.")
    public static double mod(
        Object first,
        Object second)
    {
        double iFirst;
        if (!(first instanceof Number)) {
            throw new InvalidArgumentException(
                "Invalid parameter. "
                + "first parameter " + first
                + " of Mod function must be of type number");
        } else {
            iFirst = ((Number) first).doubleValue();
        }
        double iSecond;
        if (!(second instanceof Number)) {
            throw new InvalidArgumentException(
                "Invalid parameter. "
                + "second parameter " + second
                + " of Mod function must be of type number");
        } else {
            iSecond = ((Number) second).doubleValue();
        }
        // Use formula "mod(n, d) = n - d * int(n / d)".
        if (iSecond == 0) {
            throw new ArithmeticException("/ by zero");
        }
        return iFirst - iSecond * Vba.intNative(iFirst / iSecond);
    }

    // Todo: MDuration Returns the modified Macauley duration for a security
    // with an assumed par value of $100.

    // Skip: Median Returns the median of the given numbers. The median is the
    // number in the middle of a set of numbers.  Skip: Min Returns the smallest
    // number in a set of values.  Todo: MInverse Returns the inverse matrix for
    // the matrix stored in an array.

    // Todo: MIrr Returns the modified internal rate of return for a series of
    // periodic cash flows. MIRR considers both the cost of the investment and
    // the interest received on reinvestment of cash.

    // Todo: MMult Returns the matrix product of two arrays. The result is an
    // array with the same number of rows as array1 and the same number of
    // columns as array2.

    // Todo: Mode Returns the most frequently occurring, or repetitive, value in
    // an array or range of data.

    // Todo: MRound Returns a number rounded to the desired multiple.

    // Todo: MultiNomial Returns the ratio of the factorial of a sum of values
    // to the product of factorials.

    // Todo: NegBinomDist Returns the negative binomial
    // distribution. NEGBINOMDIST returns the probability that there will be
    // number_f failures before the number_s-th success, when the constant
    // probability of a success is probability_s. This function is similar to
    // the binomial distribution, except that the number of successes is fixed,
    // and the number of trials is variable. Like the binomial, trials are
    // assumed to be independent.

    // Todo: NetworkDays Returns the number of whole working days between
    // start_date and end_date. Working days exclude weekends and any dates
    // identified in holidays. Use NETWORKDAYS to calculate employee benefits
    // that accrue based on the number of days worked during a specific term.

    // Todo: Nominal Returns the nominal annual interest rate, given the
    // effective rate and the number of compounding periods per year.

    // Todo: NormDist Returns the normal distribution for the specified mean and
    // standard deviation. This function has a very wide range of applications
    // in statistics, including hypothesis testing.

    // Todo: NormInv Returns the inverse of the normal cumulative distribution
    // for the specified mean and standard deviation.

    // Todo: NormSDist Returns the standard normal cumulative distribution
    // function. The distribution has a mean of 0 (zero) and a standard
    // deviation of one. Use this function in place of a table of standard
    // normal curve areas.

    // Todo: NormSInv Returns the inverse of the standard normal cumulative
    // distribution. The distribution has a mean of zero and a standard
    // deviation of one.

    // Todo: NPer Returns the number of periods for an investment based on
    // periodic, constant payments and a constant interest rate.

    // Todo: Npv Calculates the net present value of an investment by using a
    // discount rate and a series of future payments (negative values) and
    // income (positive values).

    // Todo: Oct2Bin Converts an octal number to binary.

    // Todo: Oct2Dec Converts an octal number to decimal.

    // Todo: Oct2Hex Converts an octal number to hexadecimal.

    // Todo: Odd Returns number rounded up to the nearest odd integer.

    // Todo: OddFPrice Returns the price per $100 face value of a security
    // having an odd (short or long) first period.

    // Todo: OddFYield Returns the yield of a security that has an odd (short or
    // long) first period.

    // Todo: OddLPrice Returns the price per $100 face value of a security
    // having an odd (short or long) last coupon period.

    // Todo: OddLYield Returns the yield of a security that has an odd (short or
    // long) last period.

    // Skip: Or Returns TRUE if any argument is TRUE; returns FALSE if all
    // arguments are FALSE.  Todo: Pearson Returns the Pearson product moment
    // correlation coefficient, r, a dimensionless index that ranges from -1.0
    // to 1.0 inclusive and reflects the extent of a linear relationship between
    // two data sets.


    // We have a more efficient implementation of percentile

    // Skip: Percentile Returns the k-th percentile of values in a range. You
    // can use this function to establish a threshold of acceptance. For
    // example, you can decide to examine candidates who score above the 90th
    // percentile.

    // Todo: PercentRank Returns the rank of a value in a data set as a
    // percentage of the data set. This function can be used to evaluate the
    // relative standing of a value within a data set. For example, you can use
    // PERCENTRANK to evaluate the standing of an aptitude test score among all
    // scores for the test.

    // Todo: Permut Returns the number of permutations for a given number of
    // objects that can be selected from number objects. A permutation is any
    // set or subset of objects or events where internal order is
    // significant. Permutations are different from combinations, for which the
    // internal order is not significant. Use this function for lottery-style
    // probability calculations.

    // Todo: Phonetic Extracts the phonetic (furigana) characters from a text
    // string.


    @FunctionName("Pi")
    @Description(
        "Returns the number 3.14159265358979, the mathematical constant pi, "
        + "accurate to 15 digits.")
    public static double pi() {
        return Math.PI;
    }

    // Todo: Pmt Calculates the payment for a loan based on constant payments
    // and a constant interest rate.

    // Todo: Poisson Returns the Poisson distribution. A common application of
    // the Poisson distribution is predicting the number of events over a
    // specific time, such as the number of cars arriving at a toll plaza in 1
    // minute.


    @FunctionName("Power")
    @Description("Returns the result of a number raised to a power.")
    public static double power(double x, double y) {
        return Math.pow(x, y);
    }

    // Todo: Ppmt Returns the payment on the principal for a given period for an
    // investment based on periodic, constant payments and a constant interest
    // rate.

    // Todo: Price Returns the price per $100 face value of a security that pays
    // periodic interest.

    // Todo: PriceDisc Returns the price per $100 face value of a discounted
    // security.

    // Todo: PriceMat Returns the price per $100 face value of a security that
    // pays interest at maturity.

    // Todo: Prob Returns the probability that values in a range are between two
    // limits. If upper_limit is not supplied, returns the probability that
    // values in x_range are equal to lower_limit.

    // Todo: Product Multiplies all the numbers given as arguments and returns
    // the product.

    // Todo: Proper Capitalizes the first letter in a text string and any other
    // letters in text that follow any character other than a letter. Converts
    // all other letters to lowercase letters.

    // Todo: Pv Returns the present value of an investment. The present value is
    // the total amount that a series of future payments is worth now. For
    // example, when you borrow money, the loan amount is the present value to
    // the lender.

    // Todo: Quartile Returns the quartile of a data set. Quartiles often are
    // used in sales and survey data to divide populations into groups. For
    // example, you can use QUARTILE to find the top 25 percent of incomes in a
    // population.

    // Todo: Quotient Returns the integer portion of a division. Use this
    // function when you want to discard the remainder of a division.


    @FunctionName("Radians")
    @Description("Converts degrees to radians.")
    public static double radians(double number) {
        // 180 degrees = Pi radians
        return number / 180.0 * Math.PI;
    }

    // Todo: RandBetween Returns a random integer number between the numbers you
    // specify. A new random integer number is returned every time the worksheet
    // is calculated.

    // Skip: Rank Returns the rank of a number in a list of numbers. The rank of
    // a number is its size relative to other values in a list. (If you were to
    // sort the list, the rank of the number would be its position.)  Todo: Rate
    // Returns the interest rate per period of an annuity. RATE is calculated by
    // iteration and can have zero or more solutions. If the successive results
    // of RATE do not converge to within 0.0000001 after 20 iterations, RATE
    // returns the #NUM! error value.

    // Todo: Received Returns the amount received at maturity for a fully
    // invested security.

    // Todo: Replace Replaces part of a text string, based on the number of
    // characters you specify, with a different text string.

    // Todo: ReplaceB REPLACEB replaces part of a text string, based on the
    // number of bytes you specify, with a different text string.

    // Todo: Rept Repeats text a given number of times. Use REPT to fill a cell
    // with a number of instances of a text string.

    // Todo: Roman Converts an arabic numeral to roman, as text.

    // Todo: Round Rounds a number to a specified number of digits.

    // Todo: RoundDown Rounds a number down, toward zero.

    // Todo: RoundUp Rounds a number up, away from 0 (zero).

    // Todo: RSq Returns the square of the Pearson product moment correlation
    // coefficient through data points in known_y's and known_x's. For more
    // information, see PEARSON. The r-squared value can be interpreted as the
    // proportion of the variance in y attributable to the variance in x.

    // Todo: RTD This method connects to a source to receive real-time data.

    // Todo: Search SEARCH and SEARCHB locate one text string within a second
    // text string, and return the number of the starting position of the first
    // text string from the first character of the second text string.

    // Todo: SearchB SEARCH and SEARCHB locate one text string within a second
    // text string, and return the number of the starting position of the first
    // text string from the first character of the second text string.

    // Todo: SeriesSum Returns the sum of a power series based on the formula:

    // Todo: Sinh Returns the hyperbolic sine of a number.


    @FunctionName("Sinh")
    @Description("Returns the hyperbolic sine of a number.")
    public static double sinh(double number) {
        return Math.sinh(number);
    }

    // Todo: Skew Returns the skewness of a distribution. Skewness characterizes
    // the degree of asymmetry of a distribution around its mean. Positive
    // skewness indicates a distribution with an asymmetric tail extending
    // toward more positive values. Negative skewness indicates a distribution
    // with an asymmetric tail extending toward more negative values.

    // Todo: Sln Returns the straight-line depreciation of an asset for one
    // period.

    // Todo: Slope Returns the slope of the linear regression line through data
    // points in known_y's and known_x's. The slope is the vertical distance
    // divided by the horizontal distance between any two points on the line,
    // which is the rate of change along the regression line.

    // Todo: Small Returns the k-th smallest value in a data set. Use this
    // function to return values with a particular relative standing in a data
    // set.


    @FunctionName("SqrtPi")
    @Description("Returns the square root of (number * pi).")
    public static double sqrtPi(double number) {
        return Math.sqrt(number * Math.PI);
    }

    // Todo: Standardize Returns a normalized value from a distribution
    // characterized by mean and standard_dev.

    // Todo: StDev Estimates standard deviation based on a sample. The standard
    // deviation is a measure of how widely values are dispersed from the
    // average value (the mean).

    // Todo: StDevP Calculates standard deviation based on the entire population
    // given as arguments. The standard deviation is a measure of how widely
    // values are dispersed from the average value (the mean).

    // Todo: StEyx Returns the standard error of the predicted y-value for each
    // x in the regression. The standard error is a measure of the amount of
    // error in the prediction of y for an individual x.

    // Todo: Substitute Substitutes new_text for old_text in a text string. Use
    // SUBSTITUTE when you want to replace specific text in a text string; use
    // REPLACE when you want to replace any text that occurs in a specific
    // location in a text string.

    // Todo: Subtotal Creates subtotals.

    // Todo: Sum Adds all the numbers in a range of cells.

    // Todo: SumIf Adds the cells specified by a given criteria.

    // Todo: SumIfs Adds the cells in a range that meet multiple criteria.

    // Todo: SumProduct Multiplies corresponding components in the given arrays,
    // and returns the sum of those products.

    // Todo: SumSq Returns the sum of the squares of the arguments.

    // Todo: SumX2MY2 Returns the sum of the difference of squares of
    // corresponding values in two arrays.

    // Todo: SumX2PY2 Returns the sum of the sum of squares of corresponding
    // values in two arrays. The sum of the sum of squares is a common term in
    // many statistical calculations.

    // Todo: SumXMY2 Returns the sum of squares of differences of corresponding
    // values in two arrays.

    // Todo: Syd Returns the sum-of-years' digits depreciation of an asset for a
    // specified period.


    @FunctionName("Tanh")
    @Description("Returns the hyperbolic tangent of a number.")
    public static double tanh(double number) {
        return Math.tanh(number);
    }

    // Todo: TBillEq Returns the bond-equivalent yield for a Treasury bill.

    // Todo: TBillPrice Returns the price per $100 face value for a Treasury
    // bill.

    // Todo: TBillYield Returns the yield for a Treasury bill.

    // Todo: TDist Returns the Percentage Points (probability) for the Student
    // t-distribution where a numeric value (x) is a calculated value of t for
    // which the Percentage Points are to be computed. The t-distribution is
    // used in the hypothesis testing of small sample data sets. Use this
    // function in place of a table of critical values for the t-distribution.

    // Todo: Text Converts a value to text in a specific number format.

    // Todo: TInv Returns the t-value of the Student's t-distribution as a
    // function of the probability and the degrees of freedom.

    // Todo: Transpose Returns a vertical range of cells as a horizontal range,
    // or vice versa. TRANSPOSE must be entered as an array formula in a range
    // that has the same number of rows and columns, respectively, as an array
    // has columns and rows. Use TRANSPOSE to shift the vertical and horizontal
    // orientation of an array on a worksheet.

    // Todo: Trend Returns values along a linear trend. Fits a straight line
    // (using the method of least squares) to the arrays known_y's and
    // known_x's. Returns the y-values along that line for the array of new_x's
    // that you specify.

    // Todo: Trim Removes all spaces from text except for single spaces between
    // words. Use TRIM on text that you have received from another application
    // that may have irregular spacing.

    // Todo: TrimMean Returns the mean of the interior of a data set. TRIMMEAN
    // calculates the mean taken by excluding a percentage of data points from
    // the top and bottom tails of a data set. You can use this function when
    // you wish to exclude outlying data from your analysis.

    // Todo: TTest Returns the probability associated with a Student's
    // t-Test. Use TTEST to determine whether two samples are likely to have
    // come from the same two underlying populations that have the same mean.

    // Todo: USDollar The description for this item will appear in the final
    // release of Office 2007.

    // Todo: Var Estimates variance based on a sample.

    // Todo: VarP Calculates variance based on the entire population.

    // Todo: Vdb Returns the depreciation of an asset for any period you
    // specify, including partial periods, using the double-declining balance
    // method or some other method you specify. VDB stands for variable
    // declining balance.

    // Todo: VLookup Searches for a value in the first column of a table array
    // and returns a value in the same row from another column in the table
    // array.

    // Todo: Weekday Returns the day of the week corresponding to a date. The
    // day is given as an integer, ranging from 1 (Sunday) to 7 (Saturday), by
    // default.

    // Todo: WeekNum Returns a number that indicates where the week falls
    // numerically within a year.

    // Todo: Weibull Returns the Weibull distribution. Use this distribution in
    // reliability analysis, such as calculating a device's mean time to
    // failure.

    // Todo: WorkDay Returns a number that represents a date that is the
    // indicated number of working days before or after a date (the starting
    // date). Working days exclude weekends and any dates identified as
    // holidays. Use WORKDAY to exclude weekends or holidays when you calculate
    // invoice due dates, expected delivery times, or the number of days of work
    // performed.

    // Todo: Xirr Returns the internal rate of return for a schedule of cash
    // flows that is not necessarily periodic. To calculate the internal rate of
    // return for a series of periodic cash flows, use the IRR function.

    // Todo: Xnpv The description for this item will appear in the final release
    // of Office 2007.

    // Todo: YearFrac Calculates the fraction of the year represented by the
    // number of whole days between two dates (the start_date and the
    // end_date). Use the YEARFRAC worksheet function to identify the proportion
    // of a whole year's benefits or obligations to assign to a specific term.

    // Todo: YieldDisc Returns the annual yield for a discounted security.

    // Todo: YieldMat Returns the annual yield of a security that pays interest
    // at maturity.

    // Todo: ZTest Returns the one-tailed probability-value of a z-test. For a
    // given hypothesized population mean, ZTEST returns the probability that
    // the sample mean would be greater than the average of observations in the
    // data set (array) -- that is, the observed sample mean.

}

// End Excel.java
