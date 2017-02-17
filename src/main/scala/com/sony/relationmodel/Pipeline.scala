package com.sony.relationmodel

import java.nio.file.{Paths, Files}

trait Task {
  def run(): Unit
}

trait Data {
  def getData(force: Boolean = false): String
  def exists(path: String): Boolean = Files.exists(Paths.get(path))
}

