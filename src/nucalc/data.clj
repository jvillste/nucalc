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
            [clojure.test :refer :all]
            [argumentica.reduction :as reduction]

            [argumentica.util :as util]
            [argumentica.btree :as btree]
            [medley.core :as medley]))

(def food-file-name "/Users/jukka/Documents/FoodData_Central_csv_2020-04-29/food.csv")
(def food-nutrient-file-name "/Users/jukka/Documents/FoodData_Central_csv_2020-04-29/food_nutrient.csv")
(def nutrient-file-name "/Users/jukka/Documents/FoodData_Central_csv_2020-04-29/nutrient.csv")

(def non-branded-foods-file-name "temp/non-branded-foods.data")
(def non-branded-food-nutrient-file-name "temp/non-branded-food-nutrients.data")
(def food-nutrient-amount-btree-file-name "temp/food-nutrient-amount")
(def food-description-token-data-type-food-btree-file-name "temp/food-token-data-type-id")
(def food-data-type-description-btree-file-name "temp/fdc-id-data-type-description")
(def nutrients-by-name-token-btree-file-name "temp/nutrients-by-name-token")
(def nutrients-by-id-file-name "temp/nutrients-by-id")

(comment
  (sample (csv/hashmap-reducible-for-csv-file nutrient-file-name
                                              {:value-function nucalc/parse-number}))

  (sample (serialization/file-reducible non-branded-foods-file-name))

  (sample (serialization/file-reducible non-branded-food-nutrient-file-name))

  ) ;; TODO: remove-me

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





  (nucalc/create-projection-btree nutrients-by-id-file-name
                                  [:id
                                   :name
                                   :unit-name
                                   :nutrient-nbr
                                   :rank]
                                  (csv/hashmap-reducible-for-csv-file nutrient-file-name
                                                                      {:value-function (comp nucalc/parse-number
                                                                                             nucalc/empty-string-to-nil)}))

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
   :nutrients-by-name-token (nucalc/open-btree nutrients-by-name-token-btree-file-name)
   :nutrients-by-id (nucalc/open-btree nutrients-by-id-file-name)})

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

(defn starts-with [string & [key]]
  {:minimum-value string
   :match (fn [value]
            (string/starts-with? value string))
   :key key})

(defn equals [key value]
  {:minimum-value value
   :match (partial = value)
   :key key})

(defn find-food-3 [indexes tokens]
  (query/merge-join (concat (for [token tokens #_(take 1 (drop 0 tokens))]
                              {:sorted-reducible (:food-description-token-data-type-food indexes)
                               :pattern [token #_(starts-with token)
                                         "sr_legacy_food" #_:?data-type
                                         :?food]})
                            [{:sorted-reducible (:food-data-type-description indexes)
                              :pattern [:?food :* :?description]}])))


(defn tokens-starting-with [food-description-token-data-type-food start-string]
  (map first
       (query/matching-unique-subsequence food-description-token-data-type-food
                                          [(starts-with start-string :?token)])))

(defn find-food-with-nested-loop-join [indexes tokens]
  (query/query-2 (into [(:food-description-token-data-type-food indexes)]
                       (for [token tokens]
                         [(starts-with token)
                          :?data-type
                          :?food]))
                 [(:food-data-type-description indexes)
                  [:?food :* :?description]]))

(defn take-until-value [queried-value sequence]
  (take-while #(not (= % queried-value))
              sequence))

(deftest test-take-until-value
  (is (= '(:a :b)
         (take-until-value :c [:a :b :c :d]))))

;; TODO: remove-me

(defn substitution-product [participants]
  (query/cartesian-product (map (fn [{:keys [sorted pattern]}]
                                  (query/unique-substitutions sorted pattern))
                                participants)))

(defn merged-substitution-product [selections]
  (map (fn [substitutions]
         (apply merge substitutions))
       (substitution-product selections)))

(defn product [participants]
  (query/cartesian-product (map (fn [{:keys [sorted pattern]}]
                                  (query/matching-unique-subsequence sorted pattern))
                                participants)))

(defn replace-pattern-prefix [prefix pattern]
  (concat (map (fn [prefix-value pattern-term]
                 (if (:key pattern-term)
                   (equals (:key pattern-term) prefix-value)
                   prefix-value))
               prefix
               pattern)
          (drop (count prefix)
                pattern)))

(deftest test-replace-prefix
  (is (= '(:a :b 3 4)
         (replace-pattern-prefix [:a :b] [1 2 3 4])))

  #_(is (= [(equals :?first "aa")
            :b
            3
            4]
           (replace-pattern-prefix ["aa" :b]
                                   [{:key :?first} 2 3 4]))))

(defn selection-product [participants]
  (let [first-common-term (first (query/common-combined-key-pattern (map :pattern participants)))]
    (for [pattern-prefixes (product (map (fn [participant]
                                           (update participant :pattern (partial take-until-value first-common-term)))
                                         participants))]
      (map (fn [participant pattern-prefix]
             (update participant :pattern (partial replace-pattern-prefix pattern-prefix)))
           participants
           pattern-prefixes))))

(defn concat-merge-joins [participant-combinations]
  (apply concat (map query/merge-join participant-combinations)))

(defn merge-join-with-prefixes [participants]
  (apply concat (map query/merge-join (selection-product participants))))

(defn loop-join [index pattern substitutions]
  (check-pattern! (:columns index) pattern)

  (apply concat (for [substitution substitutions]
                  (map (fn [new-substitution]
                         (merge substitution new-substitution))
                       (query/unique-substitutions (:sorted index)
                                                   (query/substitute pattern substitution))))))

(defn find-food-with-merge-join [indexes tokens]
  (->> (selection-product (for [token tokens]
                            {:sorted (:food-description-token-data-type-food indexes)
                             :pattern [(starts-with token)
                                       :?data-type
                                       :?food]}))
       (concat-merge-joins)
       (loop-join (:food-data-type-description indexes)
                  [:?food :?data-type :?description])))





(defn project-to-sorted-set [pattern substitutions]
  (into (db-common/create-sorted-datom-set)
        (map (apply juxt pattern)
             substitutions)))

(defn mapcat-merge-join [selections substitutions]
  (apply concat
         (for [substitution substitutions]
           (map (fn [result]
                  (merge result substitution))
                (query/merge-join (for [selection selections]
                                    (update selection :pattern #(query/substitute % substitution))))))))




(defn sorted [indexes index-key]
  (get-in indexes [index-key :sorted]))

(defn term-name [term]
  (cond (:original-key term)
        (query/unconditional-variable-name (:original-key term))

        (query/varialbe-key? term)
        (query/unconditional-variable-name term)

        (:key term)
        (query/unconditional-variable-name (:key term))

        :default
        nil))

(defn check-pattern! [columns pattern]
  (when (< (count columns)
           (count pattern))
    (throw (ex-info "Pattern is too long" {})))

  (loop [names (map name columns)
         terms pattern]
    (when-let [term (first terms)]
      (if-let [term-name (term-name term)]
        (if (= (first names)
               term-name)
          (recur (rest names)
                 (rest terms))
          (throw (ex-info "term name does not match column name" {:column-name (first names)
                                                                  :term-name term-name})))
        (recur (rest names)
               (rest terms))))))

(deftest test-check-pattern!
  (is (nil? (check-pattern! [:a :b]
                           [:?a :?b])))

  (is (nil? (check-pattern! [:a :b]
                           [:?a])))

  (is (nil? (check-pattern! [:a :b]
                           [{:original-key :?a}])))

  (is (thrown-with-msg? clojure.lang.ExceptionInfo
                        #"Pattern is too long"
                        (check-pattern! [:a :b]
                                       [:?a :?b :?c]))))

(defn select-unique [index pattern]
  (check-pattern! (:columns index) pattern)

  (query/matching-unique-subsequence (:sorted index)
                                     pattern))

(defn select [index pattern]
  (check-pattern! (:columns index) pattern)

  (let [result-row-length (count pattern)]
    (map (partial take result-row-length)
         (query/matching-subsequence (:sorted index)
                                     pattern))))

(defn unify [index pattern]
  (map #(query/unify % pattern)
       (select index pattern)))

(defn unify-unique [index pattern]
  (map #(query/unify % pattern)
       (select-unique index pattern)))

(defn rename [original-key-or-term new-key]
  (if (query/conditional-variable? original-key-or-term)
    (assoc original-key-or-term
           :original-key (:key original-key-or-term)
           :key new-key)

    {:original-key original-key-or-term
     :key new-key}))
















(defn create-in-memory-indexes []
  (let [nutrients [{:id 1003, :name "Protein", :unit-name "G", :rank 600}
                   {:id 1004, :name "Total lipid (fat)", :unit-name "G", :rank 800}
                   {:id 1005, :name "Carbohydrate, by difference", :unit-name "G", :rank 1110}]

        foods [{:data-type "sr_legacy_food", :id 170393, :description "Carrots, raw"}
               {:data-type "survey_fndds_food", :id 787522, :description "Carrots, raw"}
               {:data-type "sr_legacy_food", :id 168483, :description "Sweet potato, cooked, baked in skin, flesh, without salt"}
               {:data-type "sr_legacy_food", :id 170434, :description "Potatoes, white, flesh and skin, baked"}
               {:data-type "sr_legacy_food", :id 168409, :description "Cucumber, with peel, raw"}
               {:data-type "sr_legacy_food", :id 169975, :description "Cabbage, raw"}
               {:data-type "survey_fndds_food", :id 787782, :description "Cabbage, red, raw"}
               {:data-type "sr_legacy_food", :id 169986, :description "Cauliflower, raw"}
               {:data-type "survey_fndds_food", :id 787784, :description "Cauliflower, raw"}]

        measurements [{:nutrient 1003, :amount 0.65, :id 1358332, :food 168409}
                      {:nutrient 1004, :amount 0.11, :id 1358295, :food 168409}
                      {:nutrient 1005, :amount 3.63, :id 1358223, :food 168409}
                      {:nutrient 1003, :amount 2.01, :id 1364334, :food 168483}
                      {:nutrient 1004, :amount 0.15, :id 1364310, :food 168483}
                      {:nutrient 1005, :amount 20.71, :id 1364253, :food 168483}
                      {:nutrient 1003, :amount 1.28, :id 1484505, :food 169975}
                      {:nutrient 1004, :amount 0.1, :id 1484444, :food 169975}
                      {:nutrient 1005, :amount 5.8, :id 1484445, :food 169975}
                      {:nutrient 1003, :amount 1.92, :id 1485459, :food 169986}
                      {:nutrient 1004, :amount 0.28, :id 1485505, :food 169986}
                      {:nutrient 1005, :amount 4.97, :id 1485411, :food 169986}
                      {:nutrient 1003, :amount 0.93, :id 1519940, :food 170393}
                      {:nutrient 1004, :amount 0.24, :id 1519947, :food 170393}
                      {:nutrient 1005, :amount 9.58, :id 1519995, :food 170393}
                      {:nutrient 1003, :amount 2.1, :id 1523436, :food 170434}
                      {:nutrient 1004, :amount 0.15, :id 1523468, :food 170434}
                      {:nutrient 1005, :amount 21.08, :id 1523486, :food 170434}
                      {:nutrient 1003, :amount 0.93, :id 9489576, :food 787522}
                      {:nutrient 1004, :amount 0.24, :id 9489577, :food 787522}
                      {:nutrient 1005, :amount 9.58, :id 9489578, :food 787522}
                      {:nutrient 1003, :amount 1.43, :id 9506476, :food 787782}
                      {:nutrient 1004, :amount 0.16, :id 9506477, :food 787782}
                      {:nutrient 1005, :amount 7.37, :id 9506478, :food 787782}
                      {:nutrient 1003, :amount 1.92, :id 9506606, :food 787784}
                      {:nutrient 1004, :amount 0.28, :id 9506607, :food 787784}
                      {:nutrient 1005, :amount 4.97, :id 9506608, :food 787784}]]

    {:nutrients (nucalc/in-memory-index (nucalc/projection :id :name :unit-name :rank)
                                        nutrients)
     :foods (nucalc/in-memory-index (nucalc/projection :id :data-type :description)
                                    foods)
     :foods-by-description (nucalc/in-memory-index (nucalc/full-text [:description]
                                                                     [:data-type :id])
                                                   foods)
     :measurements (nucalc/in-memory-index (nucalc/projection :food :nutrient :amount :id)
                                           measurements)}))

(comment

  (def in-memory-indexes (create-in-memory-indexes))

  (medley/map-vals (fn [index]
                     (update index
                             :sorted
                             (partial take 3)))
                   in-memory-indexes)

  {:nutrients {:sorted ([1003 "Protein" "G" 600]
                        [1004 "Total lipid (fat)" "G" 800]
                        [1005 "Carbohydrate, by difference" "G" 1110]),
               :columns (:id :name :unit-name :rank)},

   :foods {:sorted ([168409 "sr_legacy_food" "Cucumber, with peel, raw"]
                    [168483 "sr_legacy_food" "Sweet potato, cooked, baked in skin, flesh, without salt"]
                    [169975 "sr_legacy_food" "Cabbage, raw"]),
           :columns (:id :data-type :description)},

   :foods-by-description {:sorted (["and" "sr_legacy_food" 170434]
                                   ["baked" "sr_legacy_food" 168483]
                                   ["baked" "sr_legacy_food" 170434]),
                          :columns (:token :data-type :id)},

   :measurements {:sorted ([168409 1003 0.65 1358332]
                           [168409 1004 0.11 1358295]
                           [168409 1005 3.63 1358223]),
                  :columns (:food :nutrient :amount :id)}}



  (:columns (:foods-by-description in-memory-indexes))

  (:token :data-type :id)



  (select (:foods-by-description in-memory-indexes)
          [(starts-with "r")
           :?data-type
           :?id])

  (("raw") ("raw") ("raw") ("raw") ("raw") ("raw") ("raw") ("red"))



  (select-unique (:foods-by-description in-memory-indexes)
                 [(starts-with "r")])

  (("raw") ("red"))



  (unify-unique (:foods-by-description in-memory-indexes)
                [(starts-with "r" :?token)])

  ({:?token "raw"}
   {:?token "red"})



  (unify-unique (:foods-by-description in-memory-indexes)
                [(-> (starts-with "r" :?token)
                     (rename :?description-token))])

  ({:?description-token "raw"}
   {:?description-token "red"})



  (take 4 (unify-unique (:foods-by-description in-memory-indexes)
                        [(rename :?token :?description-token)]))

  ({:?description-token "and"}
   {:?description-token "baked"}
   {:?description-token "cabbage"}
   {:?description-token "carrots"})


  (unify (:foods-by-description in-memory-indexes)
         ["cabbage"
          :*
          :?id])

  (({:?id 169975}
    {:?id 787782}))



  (->> (unify (:foods-by-description in-memory-indexes)
              ["cabbage"
               :*
               (rename :?id :?food)])
       (loop-join (:foods in-memory-indexes)
                  [(rename :?id :?food)
                   :*
                   :?description]))

  ({:?food 169975, :?description "Cabbage, raw"}
   {:?food 787782, :?description "Cabbage, red, raw"})


  )











(comment
  (select (:foods in-memory-indexes)
          [:*
           :*
           (starts-with "P")])


  ;; add check to merge join: no variables are allowed before the common sub pattern in merge join
  (->> (unify (:foods-by-description in-memory-indexes)
              ["cabbage"
               :*
               (rename :?id :?food)])
       (query/merge-join (:foods in-memory-indexes)
                         [(rename :?id :?food)
                          :*
                          :?description]))


  (def result (for [token ["r" "ca"]]
                (map first
                     (select-unique (:foods-by-description in-memory-indexes)
                                    [(starts-with token)]))))

  (def result '(("raw" "red")
                ("cabbage" "carrots" "cauliflower")))



  (def result (query/cartesian-product result))

  (def result '(("raw" "cabbage")
                ("red" "cabbage")
                ("raw" "carrots")
                ("red" "carrots")
                ("raw" "cauliflower")
                ("red" "cauliflower")))



  (def result (->> result
                   (map (fn [tokens]
                          (for [token tokens]
                            {:sorted (:foods-by-description in-memory-indexes)
                             :pattern [token
                                       :?data-type
                                       :?food]})))
                   (mapcat query/merge-join)))

  (def result '({:?data-type "sr_legacy_food", :?food 169975}
                {:?data-type "survey_fndds_food", :?food 787782}
                {:?data-type "survey_fndds_food", :?food 787782}
                {:?data-type "sr_legacy_food", :?food 170393}
                {:?data-type "survey_fndds_food", :?food 787522}
                {:?data-type "sr_legacy_food", :?food 169986}
                {:?data-type "survey_fndds_food", :?food 787784}))



  (def result (loop-join (:foods in-memory-indexes)
                         [:?food :* :?description]
                         result))

  (def result '({:?data-type "sr_legacy_food", :?food 169975, :?description "Cabbage, raw"}
                {:?data-type "survey_fndds_food", :?food 787782, :?description "Cabbage, red, raw"}
                {:?data-type "survey_fndds_food", :?food 787782, :?description "Cabbage, red, raw"}
                {:?data-type "sr_legacy_food", :?food 170393, :?description "Carrots, raw"}
                {:?data-type "survey_fndds_food", :?food 787522, :?description "Carrots, raw"}
                {:?data-type "sr_legacy_food", :?food 169986, :?description "Cauliflower, raw"}
                {:?data-type "survey_fndds_food", :?food 787784, :?description "Cauliflower, raw"}))

  ) ;; TODO: remove-me

(comment
  (def indexes (open-indexes))

  (time (let [in-memory-indexes (create-in-memory-indexes)]
          (->> (selection-product (for [token ["raw" "ca"]]
                                    {:sorted (:foods-by-description in-memory-indexes)
                                     :pattern [(starts-with token)
                                               :?data-type
                                               :?food]}))
               (map query/merge-join)
               (apply concat)
               (loop-join (:foods in-memory-indexes)
                          [:?food :* :?description]))))

  (time (let [in-memory-indexes (create-in-memory-indexes)]
          (->> (selection-product (for [token ["raw" "ca"]]
                                    {:sorted (:foods-by-description in-memory-indexes)
                                     :pattern [(starts-with token)
                                               :?data-type
                                               :?food]}))
               (map query/merge-join)
               (apply concat)
               (loop-join (:foods in-memory-indexes)
                          [:?food :* :?description]))))

  (time (let [in-memory-indexes (create-in-memory-indexes)]
          (->> (query/select (:foods in-memory-indexes)
                             [:?food])
               (loop-join (:food-nutrient-amount indexes)
                          [:?food :?nutrient :?amount :?id])
               (loop-join (:nutrients in-memory-indexes)
                          [:?nutrient])
               (take 10)
               (doall))))

  (let [in-memory-indexes (create-in-memory-indexes)]
    (mapcat (fn [food]
              (query/merge-join [{:sorted (:food-nutrient-amount indexes)
                                  :pattern [food :?nutrient :?amount :?id]}
                                 {:sorted (:nutrients in-memory-indexes)
                                  :pattern [:?nutrient]}]))
            (map :?food (query/select (:foods in-memory-indexes)
                                      [:?food]))))


  (time (vec (let [in-memory-indexes (create-in-memory-indexes)]
               (->> (query/select (:foods in-memory-indexes)
                                  [:?food])
                    (mapcat-merge-join [{:sorted (:food-nutrient-amount indexes)
                                         :pattern [:?food :?nutrient :?amount :?id]}
                                        {:sorted (:nutrients in-memory-indexes)
                                         :pattern [:?nutrient]}])
                    (query/variable-keys-to-keywords)))))


  (time (vec (let [in-memory-indexes (create-in-memory-indexes)]
               (->> (merged-substitution-product [{:sorted (:foods in-memory-indexes)
                                                   :pattern [:?food]}
                                                  {:sorted (:nutrients in-memory-indexes)
                                                   :pattern [:?nutrient]}])
                    (sort-by (juxt :?food :?nutrient))
                    (loop-join (:food-nutrient-amount indexes)
                               [:?food :?nutrient :?amount :?id])))))

  (time (vec (let [in-memory-indexes (create-in-memory-indexes)]
               (query/merge-join [{:sorted (->> (merged-substitution-product [{:sorted (:foods in-memory-indexes)
                                                                               :pattern [:?food]}
                                                                              {:sorted (:nutrients in-memory-indexes)
                                                                               :pattern [:?nutrient]}])
                                                (project-to-sorted-set [:?food :?nutrient]))
                                   :pattern [:?food :?nutrient]}
                                  {:sorted (:food-nutrient-amount indexes)
                                   :pattern [:?food :?nutrient :?amount :?id]}]))))


  ;; could this be made to work?
  (let [in-memory-indexes (create-in-memory-indexes)]
    (query/merge-join [{:sorted (:foods in-memory-indexes)
                        :pattern [:?food]}
                       {:sorted (:food-nutrient-amount indexes)
                        :pattern [:?food :?nutrient :?amount :?id]}
                       {:sorted (:nutrients in-memory-indexes)
                        :pattern [:?nutrient]}]))

  (take 10 (query/select (:food-nutrient-amount indexes)
                         [:?food :?nutrient :?amount :id]))

  (:keys (:food-nutrient-amount indexes))
  ) ;; TODO: remove-me


(comment

  (into {} (for [index-key (keys indexes)]
             [index-key (get-in indexes [index-key :keys])]))

  (:keys (:food-description-token-data-type-food indexes))
  (:keys (:food-nutrient-amount indexes))

  (vec (map query/keyword-to-unconditional-variable (:keys (:nutrients-by-id indexes))))

  (->> (query/select (:food-description-token-data-type-food indexes)
                     ["carrot" :?data-type :?food])
       (loop-join (:food-data-type-description indexes)
                  [:?food :?data-type :?description])
       (loop-join (:food-nutrient-amount indexes)
                  [:?food :?nutrient-id :?amount :?measurement-id])

       (take 100))


  (->> (query/select (:food-nutrient-amount indexes)
                     [169043 :?nutrient-id :?amount :?measurement-id])
       (loop-join (:nutrients-by-id indexes)
                  [:?nutrient-id :?name :?unit-name :?nutrient-nbr :?rank])
       (take 100))

  (->> (query/select (:nutrients-by-id indexes)
                     [:?id :?name :?unit-name :?nutrient-nbr :?rank])
       (map (partial medley/map-keys query/unconditional-variable-to-keyword))
       (map #(select-keys % [:id
                             :name
                             :unit-name
                             :rank]))
       #_(map :?rank)
       (sort-by :rank)
       #_(take 100))



  (take 100 (product [{:sorted (:food-description-token-data-type-food indexes)
                       :pattern [(starts-with "rai")]}
                      {:sorted (:food-description-token-data-type-food indexes)
                       :pattern [(starts-with "carr")]}]))

  ;; TODO: figure out better name for loop-join and the terminology overall

  (->> (query/merge-join [{:sorted (:food-description-token-data-type-food indexes)
                           :pattern ["raw"
                                     :?data-type
                                     :?food]}
                          {:sorted (:food-description-token-data-type-food indexes)
                           :pattern ["carrots"
                                     :?data-type
                                     :?food]}])
       (loop-join (:food-data-type-description indexes)
                  [:?food :?data-type :?description])
       (take 100))

  (:log (btree/get-log-and-result (fn []
                                    (time (take 10 (query/merge-join [{:sorted (:food-description-token-data-type-food indexes)
                                                                       :pattern ["raw"
                                                                                 :?data-type
                                                                                 :?food]}]))))))

  (take 10 (let [tokens ["raw" "carrot"]
                 participants (for [token tokens]
                                {:sorted (:food-description-token-data-type-food indexes)
                                 :pattern [(starts-with token token)
                                           :?data-type
                                           :?food]})]
             (merge-join-with-prefixes participants)
             #_(for [participants (selection-product participants)]
                 (map :pattern participants))))


  (concat-merge-joins (selection-product (for [token ["raw" "carrot"]]
                                           {:sorted (:food-description-token-data-type-food indexes)
                                            :pattern [(starts-with token)
                                                      :?data-type
                                                      :?food]})))

  (for [participant-combination (selection-product (for [token ["raw" "carrot"]]
                                                     {:sorted (:food-description-token-data-type-food indexes)
                                                      :pattern [(starts-with token)
                                                                :?data-type
                                                                :?food]}))]
    (map :pattern participant-combination))


  (time (loop-join (:food-data-type-description indexes)
                   [:?food :?data-type :?description]
                   (concat-merge-joins (selection-product (for [token ["raw" "carrot"]]
                                                            {:sorted (:food-description-token-data-type-food indexes)
                                                             :pattern [(starts-with token)
                                                                       :?data-type
                                                                       :?food]})))))

  (product (for [token ["raw" "carrot"]]
             {:sorted (:food-description-token-data-type-food indexes)
              :pattern [(starts-with token)]}))

  (substitution-product (for [token ["raw" "carrot"]]
                          {:sorted (:food-description-token-data-type-food indexes)
                           :pattern [(starts-with token :?token)]}))

  (let [join-keys [:?data-type :?food]])

  ) ;; TODO: remove-me


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

(comment


  (take 10 (subseq (:food-description-token-data-type-food indexes)
                   >=
                   ["carrot" "sr_legacy_food" #_ ::comparator/max]))

  (->> (:log (btree/get-log-and-result (fn []
                                         (doall (take 1000 (query/cartesian-product [(query/unique-substitutions (:food-description-token-data-type-food indexes)
                                                                                                                 [(starts-with "ra" :?token)])
                                                                                     (query/unique-substitutions (:food-description-token-data-type-food indexes)
                                                                                                                 [(starts-with "ca" :?token)])]))))))
       (filter (comp #{:load-node-3}
                     first)))

  (substitution-product (for [token ["raw" "carrot"]]
                          {:sorted (:food-description-token-data-type-food indexes)
                           :pattern [(starts-with token :?token)]}))

  (count (query/cartesian-product [(query/unique-substitutions (:food-description-token-data-type-food indexes)
                                                               [(starts-with "raw" :?token)])
                                   (query/unique-substitutions (:food-description-token-data-type-food indexes)
                                                               [(starts-with "carrot" :?token)])]))

  (take 10 (query/subsequence (:food-description-token-data-type-food indexes)
                              [(starts-with "carrot" :?token)
                               :?data-type]))
  ;; => (["carrot" "sr_legacy_food" 169043]
  ;;     ["carrot" "sr_legacy_food" 170491]
  ;;     ["carrot" "sr_legacy_food" 170500]
  ;;     ["carrot" "sr_legacy_food" 171184]
  ;;     ["carrot" "sr_legacy_food" 173487]
  ;;     ["carrot" "sr_legacy_food" 174932]
  ;;     ["carrot" "survey_fndds_food" 784743]
  ;;     ["carrot" "survey_fndds_food" 784763]
  ;;     ["carrot" "survey_fndds_food" 784764]
  ;;     ["carrot" "survey_fndds_food" 784765])

  (take 10 (query/unique-subsequence (:food-description-token-data-type-food indexes)
                                     [(starts-with "carrot" :?token)
                                      :?data-type]))
  ;; => (("carrot" "sr_legacy_food")
  ;;     ("carrot" "survey_fndds_food")
  ;;     ("carrots" "foundation_food")
  ;;     ("carrots" "market_acquisition")
  ;;     ("carrots" "sample_food")
  ;;     ("carrots" "sr_legacy_food")
  ;;     ("carrots" "sub_sample_food")
  ;;     ("carrots" "survey_fndds_food")
  ;;     ("carton" "survey_fndds_food")
  ;;     ("cas" "sub_sample_food"))

  (query/matching-unique-subsequence (:food-description-token-data-type-food indexes)
                                     [(starts-with "carrot" :?token)
                                      :?data-type])
  ;; => (("carrot" "sr_legacy_food")
  ;;     ("carrot" "survey_fndds_food")
  ;;     ("carrots" "foundation_food")
  ;;     ("carrots" "market_acquisition")
  ;;     ("carrots" "sample_food")
  ;;     ("carrots" "sr_legacy_food")
  ;;     ("carrots" "sub_sample_food")
  ;;     ("carrots" "survey_fndds_food"))

  (query/unique-substitutions (:food-description-token-data-type-food indexes)
                              [(starts-with "carrot" :?token)
                               :?data-type])
  ;; => ({:?token "carrot", :?data-type "sr_legacy_food"}
  ;;     {:?token "carrot", :?data-type "survey_fndds_food"}
  ;;     {:?token "carrots", :?data-type "foundation_food"}
  ;;     {:?token "carrots", :?data-type "market_acquisition"}
  ;;     {:?token "carrots", :?data-type "sample_food"}
  ;;     {:?token "carrots", :?data-type "sr_legacy_food"}
  ;;     {:?token "carrots", :?data-type "sub_sample_food"}
  ;;     {:?token "carrots", :?data-type "survey_fndds_food"})

  (def test-food-description-token-data-type-food
    (util/row-set ["raw" 1 1]
                  ["carrot" 1 1]
                  ["carrots" 1 2]))

  (def index (:food-description-token-data-type-food indexes))

  (time (query/merge-join [{:sorted index
                            :pattern ["raw"
                                      :?data-type
                                      :?food]}
                           {:sorted index
                            :pattern ["carrots"
                                      :?data-type
                                      :?food]}]))

  (time (tokens-starting-with index "raw"))

  (tokens-starting-with "raw")
  (tokens-starting-with "carrot")

  (into #{}
        (map :?food)
        (query/substitution-reducible (:food-description-token-data-type-food indexes)
                                      ["raw" :* :?food]))

  (reduce reduction/value-count
          0
          (query/substitution-reducible (:food-description-token-data-type-food indexes)
                                        [(starts-with "raw") :* :?food]))

  (let [raw-foods (into #{}
                        (map :?food)
                        (query/substitution-reducible (:food-description-token-data-type-food indexes)
                                                      ["raw" :* :?food]))]

    (count (into []
                 (comp (map :?food)
                       #_(filter raw-foods))
                 (query/substitution-reducible (:food-description-token-data-type-food indexes)
                                               ["carrots" :* :?food]))))

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




  (time (sample (find-food-3 indexes
                             ["carrots" "raw"])))

  (time (into []
              (take 10)
              (find-food-3 indexes
                           ["carrots" "raw"])))

  (time (find-food-with-nested-loop-join indexes
                                         ["carrots" "raw"]))
  ;; "Elapsed time: 2149.933034 msecs"
  ;; => ({:?data-type "sr_legacy_food",
  ;;      :?food 168568,
  ;;      :?description "Carrots, baby, raw"}
  ;;     {:?data-type "sr_legacy_food",
  ;;      :?food 170393,
  ;;      :?description "Carrots, raw"}
  ;;     {:?data-type "survey_fndds_food",
  ;;      :?food 787522,
  ;;      :?description "Carrots, raw"}
  ;;     {:?data-type "survey_fndds_food",
  ;;      :?food 787523,
  ;;      :?description "Carrots, raw, salad"}
  ;;     {:?data-type "survey_fndds_food",
  ;;      :?food 787524,
  ;;      :?description "Carrots, raw, salad with apples"})

  (take 50 (time (find-food-with-merge-join indexes
                                            ["white" "potato"])))


  (->> [{:?data-type "sr_legacy_food", :?food 170393, :?description "Carrots, raw"}
        {:?data-type "survey_fndds_food", :?food 787522, :?description "Carrots, raw"}
        {:?data-type "sr_legacy_food", :?food 168483, :?description "Sweet potato, cooked, baked in skin, flesh, without salt"}
        {:?data-type "sr_legacy_food", :?food 170434, :?description "Potatoes, white, flesh and skin, baked"}
        {:?data-type "sr_legacy_food", :?food 168409, :?description "Cucumber, with peel, raw"}
        {:?data-type "sr_legacy_food", :?food 169975, :?description "Cabbage, raw"}
        {:?data-type "survey_fndds_food", :?food 787782, :?description "Cabbage, red, raw"}
        {:?data-type "sr_legacy_food", :?food 169986, :?description "Cauliflower, raw"}
        {:?data-type "survey_fndds_food", :?food 787784, :?description "Cauliflower, raw"}]
       (map (partial medley/map-keys query/unconditional-variable-to-keyword)))

  ;; "Elapsed time: 181.050454 msecs"
  ;; => (({:?food 168568,
  ;;       :?data-type "sr_legacy_food",
  ;;       :?description "Carrots, baby, raw"})
  ;;     ({:?food 170393,
  ;;       :?data-type "sr_legacy_food",
  ;;       :?description "Carrots, raw"})
  ;;     ({:?food 787522,
  ;;       :?data-type "survey_fndds_food",
  ;;       :?description "Carrots, raw"})
  ;;     ({:?food 787523,
  ;;       :?data-type "survey_fndds_food",
  ;;       :?description "Carrots, raw, salad"})
  ;;     ({:?food 787524,
  ;;       :?data-type "survey_fndds_food",
  ;;       :?description "Carrots, raw, salad with apples"}))

  (count (into [] (find-food-with-nested-loop-join indexes
                                                   ["carrots"])))

  (count (into [] (find-food-with-nested-loop-join indexes
                                                   ["raw"])))

  (sample (find-food indexes
                     ["carrots" "cooked"]))


  (sample (find-food indexes ["tomato"]))

  (sample (sorted-reducible/subreducible (:food-description-token-data-type-food indexes)
                                         ["tomato"]))

  (sample (sorted-reducible/subreducible (:food-nutrient-amount indexes)
                                         [167704 1005]))

  (sample (sorted-reducible/subreducible (:food-data-type-description indexes)
                                         [167704]))



  [{:?data-type "sr_legacy_food",
    :?food 168568,
    :?description "Carrots, baby, raw"}
   {:?data-type "sr_legacy_food",
    :?food 170393,
    :?description "Carrots, raw"}
   {:?data-type "survey_fndds_food",
    :?food 787522,
    :?description "Carrots, raw"}
   {:?data-type "survey_fndds_food",
    :?food 787523,
    :?description "Carrots, raw, salad"}
   {:?data-type "survey_fndds_food",
    :?food 787524,
    :?description "Carrots, raw, salad with apples"}]

  [{:id 1003, :name "Protein", :unit-name "G", :rank 600}
   {:id 1004, :name "Total lipid (fat)", :unit-name "G", :rank 800}
   {:id 1005, :name "Carbohydrate, by difference", :unit-name "G", :rank 1110}]

  (
   {:id 1003, :name "Protein", :unit-name "G", :rank 600}
   {:id 1004, :name "Total lipid (fat)", :unit-name "G", :rank 800}
   {:id 1005, :name "Carbohydrate, by difference", :unit-name "G", :rank 1110}
   {:id 1079, :name "Fiber, total dietary", :unit-name "G", :rank 1200}
   {:id 1082, :name "Fiber, soluble", :unit-name "G", :rank 1240}
   {:id 1084, :name "Fiber, insoluble", :unit-name "G", :rank 1260}
   {:id 2000, :name "Sugars, total including NLEA", :unit-name "G", :rank 1510}

   {:id 1087, :name "Calcium, Ca", :unit-name "MG", :rank 5300}
   {:id 1089, :name "Iron, Fe", :unit-name "MG", :rank 5400}
   {:id 1090, :name "Magnesium, Mg", :unit-name "MG", :rank 5500}
   {:id 1091, :name "Phosphorus, P", :unit-name "MG", :rank 5600}
   {:id 1092, :name "Potassium, K", :unit-name "MG", :rank 5700}
   {:id 1093, :name "Sodium, Na", :unit-name "MG", :rank 5800}
   {:id 1095, :name "Zinc, Zn", :unit-name "MG", :rank 5900}
   {:id 1098, :name "Copper, Cu", :unit-name "MG", :rank 6000}

   {:id 1258, :name "Fatty acids, total saturated", :unit-name "G", :rank 9700}
   {:id 1292, :name "Fatty acids, total monounsaturated", :unit-name "G", :rank 11400}
   {:id 1293, :name "Fatty acids, total polyunsaturated", :unit-name "G", :rank 12900}
   {:id 1257, :name "Fatty acids, total trans", :unit-name "G", :rank 15400}
   )

  ) ;; TODO: remove-me
