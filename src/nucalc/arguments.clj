(ns nucalc.arguments
  (:require [argumentica.map-to-transaction :as map-to-transaction]))

(defmacro ns-alias [alias namespace]
  `(do (create-ns (quote ~namespace))
       (alias (quote ~alias) (quote ~namespace))))

(ns-alias a argupedia)

;; (create-ns (quote argupedia))
;; (alias 'a 'argupedia)

(comment
  (map-to-transaction/maps-to-transaction
   {::a/label-en "Glycogen"
    ::a/label-fi "Glykogeeni"
    ::a/id :glycogen})

     #_{:dali/id :my/how-focus-movement
    :argupedia/description "Miten palovaroitin saadaan puolen metrin päähän ledistä?"
    :argupedia/answers #{{:argupedia/description "Jatketaan rasiassa ja laitetaan pintavetona ja pinta-asennuksena"
                          :argupedia/pros #{{:argupedia/description "Helppoa"}}
                          :argupedia/cons #{{:argupedia/description "Johto on ruma"
                                             :argupedia/premises-order [1 2 3]
                                             :argupedia/premises #{{:dali/id 1
                                                                    :argupedia/description "Näkyville jäävä johto on ruma"}
                                                                   {:dali/id 2
                                                                    :argupedia/description "Talo on uusi"}
                                                                   {:dali/id 3
                                                                    :argupedia/description "Uuteen taloon ei haluta rumia johtoja"}}}}}
                         {:argupedia/description "Jatketaan rasiassa ja upotetaan"
                          :argupedia/pros #{{:argupedia/description "Helppoa"}}
                          :argupedia/cons #{{:argupedia/description "Pora voi osua johtoon"}}}}
    }
  ) ;; TODO: remove-me
