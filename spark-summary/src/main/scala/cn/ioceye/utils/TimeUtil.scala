package cn.ioceye.utils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, GregorianCalendar}


object TimeUtil {
  val DAY_FORMAT = new SimpleDateFormat("yyyy-MM-dd")

  /**
    * locate at current week of date, and point to monday
    * @param date this date
    * @return dot time
    */
  def currentWeekOfMonday(date: Date): DotTime = {
    nthWeekOfWhichDay(date)
  }

  /**
    * locate at current week of date, and point to sunday
    * @param date this date
    * @return dot time
    */
  def currentWeekOfSunday(date: Date): DotTime = {
    nthWeekOfWhichDay(date, 0 , 7)
  }

  private[this] def nthWeekOfWhichDay(date: Date, nth: Int = 0, whichDay: Int = 1): DotTime = {
    val calendar = new GregorianCalendar

    // initial time
    calendar.setTimeInMillis(date.getTime)
    calendar.add(Calendar.WEEK_OF_YEAR, -nth)
    calendar.set(Calendar.HOUR_OF_DAY, 0)
    calendar.set(Calendar.MINUTE, 0)
    calendar.set(Calendar.SECOND, 0)
    calendar.set(Calendar.MILLISECOND, 0)

    // to monday
    var dayOfWeek = calendar.get(Calendar.DAY_OF_WEEK) - 1
    if (dayOfWeek == 0) dayOfWeek = 7
    calendar.add(Calendar.DATE, -dayOfWeek + whichDay)

    val dotTime = new DotTime
    dotTime.initField(calendar)

    dotTime
  }
}

case class DotTime(
                    var year: Int=0,
                    var month: Int=0,
                    var day: Int = 0,
                    var hour: Int = 0,
                    var minute: Int = 0,
                    var second: Int = 0,
                    var dayString: String = null) extends Serializable {
  private[this] var date: Date = _
  private[this] var calendar: Calendar = _

  def nextDay(): DotTime = {
    calendar.add(Calendar.DAY_OF_YEAR, 1)
    initField(calendar)
    this
  }

  def prevDay(): DotTime = {
    calendar.add(Calendar.DAY_OF_YEAR, -1)
    initField(calendar)
    this
  }

  def initField(calendar: Calendar): Unit = {
    date = calendar.getTime

    // year month day
    year = calendar.get(Calendar.YEAR)
    month = calendar.get(Calendar.MONTH) + 1
    day = calendar.get(Calendar.DAY_OF_MONTH)

    // hour minute second
    hour = calendar.get(Calendar.HOUR_OF_DAY)
    minute = calendar.get(Calendar.MINUTE)
    second = calendar.get(Calendar.SECOND)

    dayString = TimeUtil.DAY_FORMAT.format(date)

    this.calendar = calendar
  }
}