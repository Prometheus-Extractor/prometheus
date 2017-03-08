package com.sony.prometheus

import java.util.regex.Pattern

/**
  * Created by erik on 2017-02-21.
  */
object Filters {

  val wordPattern = Pattern.compile(".*(\\p{L}{1,}|\\d{1,}).*")

  def wordFilter(s: String): Boolean = wordPattern.matcher(s).matches()

}
