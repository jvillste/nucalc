(ns nucalc.csv
  (:require [clojure.java.io :as io]
            [clojure.data.csv :as data-csv]
            [argumentica.reduction :as reduction]
            [camel-snake-kebab.core :as camel-snake-kebab]
            [clojure.test :refer :all]
            [schema.core :as schema]
            [argumentica.util :as util]))

(defn reduce-lines [reader reducing-function initial-value]
  (try
    (reduce reducing-function
            initial-value
            (line-seq reader))
    (finally (.close reader))))

(defn line-reducible [reader]
  (reduction/reducible (partial reduce-lines reader)))

(defn string-reader [string]
  (io/reader (.getBytes string)))

(defn reduce-csv [reader reducing-function initial-value]
  (try
    (reduce reducing-function
            initial-value
            (data-csv/read-csv reader))
    (finally (.close reader))))

(defn vector-reducible [reader]
  (reduction/reducible (partial reduce-csv reader)))

(def csv-rows-to-maps-options {(schema/optional-key :value-function) fn?
                               (schema/optional-key :key-function) fn?})

(util/defno csv-rows-to-maps [{:keys [value-function
                                      key-function]
                               :or {value-function identity
                                    key-function camel-snake-kebab/->kebab-case-keyword}}
                              :- csv-rows-to-maps-options]
  (fn [rf]
    (let [keys (volatile! nil)]
      (fn
        ([result]
         (rf result))

        ([result row]
         (if (nil? @keys)
           (do (vreset! keys (map key-function row))
               result)
           (rf result
               (zipmap @keys (map value-function row)))))))))

(util/defno hashmap-reducible-from-csv [reader options :- csv-rows-to-maps-options]
  (eduction (csv-rows-to-maps options)
            (vector-reducible reader)))

(deftest test-hashmap-reducible-from-csv
  (is (= [{:a "1", :b "2"}]
         (into [] (hashmap-reducible-from-csv (string-reader "a,b\n1,2")))))

  (is (= [{"a" 1, "b" 2}]
         (into [] (hashmap-reducible-from-csv (string-reader "a,b\n1,2")
                                              {:value-function read-string
                                               :key-function identity})))))

(comment

  (into []
        (take 10)
        (hashmap-reducible-from-csv (io/reader "/Users/jukka/Documents/FoodData_Central_csv_2020-04-29/food.csv")))

  )
