package com.sparkmovie.utils

import play.api.libs.json._
import play.api.libs.json.Reads._

object MovieUtils {

    case class Review (
        author : String,
        id : String,
        url : String,
        content : String
    )
    object Review {
        implicit val reviewFormat = Json.format[Review]
    }

	case class MovieRaw (
        id : Int,
        original_title : String,
		title : String,
        overview : String,
        original_language : String,
		release_date : String,
        popularity : Double,
        vote_average : Double,
		vote_count : Int,
        reviews : List[Review],
		genre_ids : List[Int],
        video : Boolean
    )
    object MovieRaw {
        implicit val movieFormat = Json.format[MovieRaw]
    }

    /*case class Movie (

    )*/

}
