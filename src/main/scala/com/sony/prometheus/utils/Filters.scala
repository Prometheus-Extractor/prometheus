package com.sony.prometheus.utils

import java.util.regex.Pattern


object Filters {

  val wordPattern = Pattern.compile(".*(\\p{L}{1,}|\\d{1,}).*")

  def wordFilter(s: String): Boolean = wordPattern.matcher(s).matches()

}
