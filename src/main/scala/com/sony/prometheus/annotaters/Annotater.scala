package com.sony.prometheus.annotaters

import se.lth.cs.docforia.Document

/** Annotates strings into docforia Documents
 */
trait Annotater {

  /** Returns an annotated docforia Document from input String
    *
    * @param input    the input to annotate into a Document
    * @param lang     the language - defaults to Swedish
    * @param conf     the configuration - defaults to "default"
    * @returns        an annotated docforia Document
   */
  def annotatedDocument(input: String, lang: String = "sv", conf: String = "default"): Document
}

