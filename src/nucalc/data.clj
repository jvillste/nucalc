(ns nucalc.data
  (:require [nucalc.core :as nucalc]
            [argumentica.db.common :as db-common]
            [nucalc.serialization :as serialization]
            [argumentica.db.query :as query]
            [nucalc.csv :as csv]
            [argumentica.sorted-reducible :as sorted-reducible]
            [clojure.string :as string]
            [clojure.set :as set]
            [argumentica.comparator :as comparator]
            [clojure.test :refer :all]))

(def food-file-name "/Users/jukka/Documents/FoodData_Central_csv_2020-04-29/food.csv")
(def food-nutrient-file-name "/Users/jukka/Documents/FoodData_Central_csv_2020-04-29/food_nutrient.csv")
(def nutrient-file-name "/Users/jukka/Documents/FoodData_Central_csv_2020-04-29/nutrient.csv")

(def non-branded-foods-file-name "temp/non-branded-foods.data")
(def non-branded-food-nutrient-file-name "temp/non-branded-food-nutrients.data")
(def food-nutrient-amount-btree-file-name "temp/food-nutrient-amount")
(def food-description-token-data-type-food-btree-file-name "temp/food-token-data-type-id")
(def food-data-type-description-btree-file-name "temp/fdc-id-data-type-description")
(def nutrients-by-name-token-btree-file-name "temp/nutrients-by-name-token")

(comment
  (into []
        (map :unit-name)
        (csv/hashmap-reducible-for-csv-file nutrient-file-name))

  (create-projection-btree food-data-type-description-btree-file-name
                           [:fdc-id :data-type :description]
                           (serialization/file-reducible non-branded-foods-file-name))

  (nucalc/create-full-text-btree nutrients-by-name-token-btree-file-name
                                 [:name]
                                 [:id
                                  :name
                                  :unit-name
                                  :nutrient-nbr
                                  :rank]
                                 (csv/hashmap-reducible-for-csv-file nutrient-file-name
                                                                     {:value-function nucalc/parse-number}))

  (nucalc/write-csv-rows-to-data-file food-file-name
                                      non-branded-foods-file-name
                                      (filter (fn [food]
                                                (not (= "branded_food"
                                                        (:data-type food))))))

  (let [non-branded-food-id-set (into #{}
                                      (comp #_(take 3000)
                                            (map :fdc-id))
                                      (serialization/file-reducible non-branded-foods-file-name))]
    (write-csv-rows-to-data-file food-nutrient-file-name
                                 non-branded-food-nutrient-file-name
                                 (filter (fn [food-nutrient]
                                           (contains? non-branded-food-id-set
                                                      (:fdc-id food-nutrient))))))

  (nucalc/create-projection-btree food-nutrient-amount-btree-file-name
                                  [:fdc-id
                                   :nutrient-id
                                   :amount
                                   :id]
                                  (serialization/file-reducible non-branded-food-nutrient-file-name)
                                  #_(eduction (take 2) (serialization/file-reducible non-branded-food-nutrient-file-name)))


  (nucalc/create-btree food-description-token-data-type-food-btree-file-name
                       (fn [food]
                         (for [token (db-common/tokenize (:description food))]
                           [token
                            (:data-type food)
                            (:fdc-id food)]))
                       [:token :data-type :fdc-id]
                       (serialization/file-reducible non-branded-foods-file-name))

  (create-projection-btree food-data-type-description-btree-file-name
                           [:fdc-id :data-type :description]
                           (serialization/file-reducible non-branded-foods-file-name))
  )

(defn open-indexes []
  {:food-nutrient-amount (nucalc/open-btree food-nutrient-amount-btree-file-name)
   :food-description-token-data-type-food (nucalc/open-btree food-description-token-data-type-food-btree-file-name)
   :food-data-type-description (nucalc/open-btree food-data-type-description-btree-file-name)
   :nutrients-by-name-token (nucalc/open-btree nutrients-by-name-token-btree-file-name)})

(defn find-food [indexes tokens]
  (query/reducible-query nil
                         (into [(:food-description-token-data-type-food indexes)]
                               (for [token tokens]
                                 [token :?data-type :?food]))
                         [(:food-nutrient-amount indexes)
                          [:?food 1005 :?amount]]
                         [(:food-data-type-description indexes)
                          [:?food :* :?description]]))

(defn token-start-reducible [sorted-reducible query-string]
  (eduction (take-while (fn [row]
                          (string/starts-with? (first row)
                                               query-string)))
            (sorted-reducible/subreducible sorted-reducible
                                           [query-string])))

(defn starts-with [string]
  {:minimum-value string
   :match (fn [value]
            (string/starts-with? value string))})

(defn find-food-2 [indexes tokens]
  (query/reducible-query nil
                         (into [(:food-description-token-data-type-food indexes)]
                               (for [token tokens #_(take 1 (drop 0 tokens))]
                                 [(starts-with token)
                                  :?data-type
                                  :?food]))
                         [(:food-data-type-description indexes)
                          [:?food :* :?description]]))


(defn btree-rows-to-maps [btree row-reducible]
  (eduction (nucalc/rows-to-maps (:keys btree))
            row-reducible))

(defn sample [reducible]
  (into []
        (take 10)
        reducible))

(defn find-nutrients [nutrients-by-name-token query-string]
  (apply set/intersection
         (for [query-token (string/split query-string
                                         #"\s")]
           (into #{}
                 (map rest)
                 (token-start-reducible nutrients-by-name-token
                                        query-token)))))

(defn find-nutrients-2 [nutrients-by-name-token query-string]
  (query/reducible-query nil
                         (into [nutrients-by-name-token]
                               (for [token (string/split query-string
                                                         #"\s")]
                                 [{:value token
                                   :match (fn [token value]
                                            (string/starts-with? value token))}
                                  :?id
                                  :?name]))))

(defn variable-keys [pattern]
  (->> pattern
       (filter query/variable?)
       (map query/variable-key)))

(defn- common-variables
  [participants]
  (->> participants
       (map :pattern)
       (map variable-keys)
       (map set)
       (apply set/intersection)))

(defn hash-join [participants]
  (loop [first-participant (first participants)
         participants (rest participants)
         substitutions []]
    (when-let [second-participant (first participants)]
      (let [common-variables (common-variables [first-participant
                                                second-participant])
            first-participant-substitutions (into [] (query/substitution-reducible (:sorted-reducible first-participant)
                                                                                   (:pattern first-participant)))
            join-set (->> first-participant-substitutions
                          (map #(select-keys % common-variables))
                          (set))

            second-participant-substitutions (into [] (query/substitution-reducible (:sorted-reducible first-participant)
                                                                                    (:pattern first-participant)))]

        (recur (first participants)
               (rest participants)
               )

        common-variables))
    )
  )

(defn row-set [& rows]
  (apply sorted-set-by
         comparator/compare-datoms
         rows))

(deftest test-hash-join
  (is (= nil
         (hash-join [{:sorted-reducible (row-set ["a" 1 :a1]
                                                 ["a" 2 :a2-1]
                                                 ["a" 2 :a2-2]
                                                 ["b" 3 :a3])
                      :pattern ["a" :?id :?a]}

                     {:sorted-reducible (row-set ["a" 1 :b1]
                                                 ["b" 2 :b2]
                                                 ["b" 3 :b3])
                      :pattern ["b" :?id :?b]}]))))

(comment
  (let [raw-foods (into #{}
                        (map :?food)
                        (query/substitution-reducible (:food-description-token-data-type-food indexes)
                                                      ["raw" :* :?food]))]

    (count (into []
                 (comp (map :?food)
                       #_(filter raw-foods))
                 (query/substitution-reducible (:food-description-token-data-type-food indexes)
                                               ["carrots" :* :?food]))))
  (count )

  (into []
        (take 2)
        (let [tokens ["carrots"]]
          (query/reducible-query nil
                                 (into [(:food-description-token-data-type-food indexes)]
                                       (for [token tokens]
                                         [{:value token
                                           :match (fn [token value]
                                                    (string/starts-with? value token))}
                                          :?data-type
                                          :?food]))
                                 [(:food-nutrient-amount indexes)
                                  [:?food 1005 :?amount]]
                                 [(:food-data-type-description indexes)
                                  [:?food :* :?description]])))

  (string/split "ir"
                #"\s")

  (into [] (find-nutrients-2 (:nutrients-by-name-token indexes)
                             "car"))

  (sample (sorted-reducible/subreducible (:nutrients-by-name-token indexes)
                                         ["ir"]))

  (sample (token-start-reducible (:nutrients-by-name-token indexes)
                                 "vit"))

  (type (:nutrients-by-name-token indexes))

  nutrients-by-name-token-btree-file-name

  (def indexes (open-indexes))

  (sample (find-food-2 indexes
                       ["carrots" "raw"]))

  (sample (find-food indexes
                     ["carrots" "cooked"]))


  (sample (find-food indexes ["tomato"]))

  (sample (sorted-reducible/subreducible (:food-description-token-data-type-food indexes)
                                         ["tomato"]))

  (sample (sorted-reducible/subreducible (:food-nutrient-amount indexes)
                                         [167704 1005]))

  (sample (sorted-reducible/subreducible (:food-data-type-description indexes)
                                         [167704]))




  ) ;; TODO: remove-me
