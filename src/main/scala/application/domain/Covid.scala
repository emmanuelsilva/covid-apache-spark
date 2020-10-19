package application.domain

import java.util.Calendar

case class Covid(isoCode: String, continent: String, country: String, date: Calendar, casesPerDay: Int, deathsPerDay: Int, population: Long)
