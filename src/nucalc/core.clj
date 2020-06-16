(ns nucalc.core
  (:require [argumentica.btree-index :as btree-index]
            [argumentica.db.common :as db-common]
            [argumentica.db.file-transaction-log :as file-transaction-log]
            [argumentica.csv :as csv]
            [clojure.java.io :as io]
            [argumentica.btree :as btree]
            [argumentica.btree-db :as btree-db]
            [argumentica.btree-index :as btree-index]
            [argumentica.csv :as csv]
            [argumentica.db.common :as db-common]
            [argumentica.db.db :as db]
            [argumentica.db.file-transaction-log :as file-transaction-log]
            [argumentica.db.server-api :as server-api]
            [argumentica.directory-storage :as directory-storage]
            [argumentica.sorted-map-transaction-log :as sorted-map-transaction-log]
            [argumentica.sorted-set-index :as sorted-set-index]
            [argumentica.storage :as storage]
            [argumentica.transaction-log :as transaction-log]
            [clojure.set :as set]
            [clojure.string :as string]
            [clojure.test :as t :refer :all]
            [cor.api :as cor-api]
            [cor.server :as server]
            [kixi.stats.core :as stats]
            [me.raynes.fs :as fs]
            [net.cgrand.xforms :as xforms]
            [argumentica.map-to-transaction :as map-to-transaction]
            [clojure.data.csv :as data-csv]
            [medley.core :as medley]
            [argumentica.db.common :as common]
            [camel-snake-kebab.core :as camel-snake-kebab]))

(defn filter-by-attributes [attributes-set index-definition]
  (assoc index-definition
         :eatcv-to-datoms
         (fn [indexes e a t c v]
           (if (contains? attributes-set
                          a)
             ((:eatcv-to-datoms index-definition) indexes e a t c v)
             []))))

(defn full-text-index [attribute]
  {:key attribute
   :eatcv-to-datoms (fn [indexes e a t c v]
                      (if (= a attribute)
                        (db-common/eatcv-to-full-text-avtec db-common/tokenize
                                                            indexes e a t c v)
                        []))
   :datom-transaction-number-index 2})

(defn create-db [path index-definitions]
  (db-common/db-from-index-definitions index-definitions
                                       (fn [index-key]
                                         (btree-index/create-directory-btree-index (str path "/" (name index-key))
                                                                                   10001))
                                       (file-transaction-log/create (str path "/transaction-log"))))

(defn create-in-memory-db [index-definitions]
  (db-common/db-from-index-definitions index-definitions
                                       (fn [index-key]
                                         (sorted-set-index/create)
                                         #_(btree-index/create-memory-btree-index 10001))
                                       (sorted-map-transaction-log/create)))

(defn create-food-db [path]
  (create-db path
             (conj db-common/base-index-definitions
                   (full-text-index :description))))

(def schema {:food {:reference? true}
             :nutrient {:reference? true}})

(defn parse-number [string]
  (try
    (Integer/parseInt string)
    (catch Exception e
      (try
        (Double/parseDouble string)
        (catch Exception e
          string)))))

(defn empty-string-to-nil [string]
  (if (= "" string)
    nil
    string))

(defn csv-rows-to-maps [& {:keys [parse-value
                                  prepare-key-name]
                           :or {parse-value identity
                                prepare-key-name identity}}]
  (fn [reducing-function]
    (let [keys (volatile! nil)]
      (completing (fn [result row]
                    (if (nil? @keys)
                      (do (vreset! keys (map (comp keyword
                                                   prepare-key-name)
                                             row))
                          result)
                      (reducing-function result
                                         (zipmap @keys (map parse-value row)))))))))

(deftest test-csv-rows-to-maps
  (is (= [{:a 1, :b 2}]
         (transduce (comp (csv-rows-to-maps :parse-value (fn [value] (Integer/parseInt value)))
                          (take 1))
                    conj
                    [["a" "b"]
                     ["1" "2"]
                     ["3" "4"]]))))

#_(defn parse-csv [file-name]
    (with-open [reader (io/reader "/Users/jukka/Downloads/FoodData_Central_csv_2019-12-17/food_nutrient.csv")]
      (->> (data-csv/read-csv reader)
           (take 10)
           (csv-rows-to-maps)
           (map (partial medley/map-vals parse-number))
           (doall))))


(defn transduce-csv [file-name transducer & {:as options}]
  (with-open [reader (io/reader file-name)]
    (let [options (merge {:reducer (constantly nil)}
                         options)
          rows (apply data-csv/read-csv reader (apply concat options))]
      (if (contains? options :initial-value)
        (transduce transducer
                   (:reducer options)
                   (:initial-value options)
                   rows)
        (transduce transducer
                   (:reducer options)
                   rows)))))

(comment
  (transduce-csv "temp/sample.csv"
                 (take 2)
                 :reducer conj)

  (transduce-csv food-file-name
                 (comp (csv-rows-to-maps :parse-value parse-number)
                       (remove (fn [food]
                                 (= "branded_food" (:data_type food))))
                       (take 10))
                 :reducer conj)
  ) ;; TODO: remove-me

(defn transact-many! [db transact]
  (transaction-log/make-transient! (:transaction-log db))

  (transact)

  (when (instance? argumentica.btree_index.BtreeIndex
                   (-> db :indexes vals first :index))
    (btree-db/store-index-roots-after-maximum-number-of-transactions db 0))

  (transaction-log/truncate! (:transaction-log db)
                             (inc (transaction-log/last-transaction-number (:transaction-log db))))
  (transaction-log/make-persistent! (:transaction-log db))
  nil)

(defn make-transact-transducer [db-atom transducer]
  (comp (csv-rows-to-maps :parse-value (comp empty-string-to-nil
                                             parse-number))
        transducer
        (map map-to-transaction/maps-to-transaction)
        (map (fn [transaction]
               (swap! db-atom db-common/transact transaction)))
        #_(map (fn [_]
                 (swap! db-atom btree-db/store-index-roots-after-maximum-number-of-transactions 10000)))))

(defn transact-csv [db-atom file-name transducer]
  (transact-many! @db-atom
                  (fn []
                    (transduce-csv file-name
                                   (make-transact-transducer db-atom transducer)))))


(defn transact-with-transducer [db-atom transducer entities]
  (transduce (comp transducer
                   (map map-to-transaction/maps-to-transaction)
                   (map (partial swap! db-atom db-common/transact)))
             (constantly nil)
             nil
             entities)
  db-atom)

#_(defn read-csv [file-name]
    (with-open [reader (io/reader file-name)]
      (doall (csv-rows-to-maps (csv/read-csv reader)))))

(defn make-id-key [a-map {:keys [id-key prefix discarded-keys]}]
  (let [a-map (assoc a-map :dali/id (str prefix (id-key a-map)))]
    (apply dissoc
           a-map
           (or discarded-keys
               [id-key]))))

(defn make-reference-id-key [map key new-key prefix]
  (assoc map new-key (str prefix (key map))))

(defn add-type [type a-map]
  (assoc a-map :type type))

(defn make-id-keys [map key-specification]
  (reduce (fn [map reference-key]
            (make-reference-id-key map
                                   (:key reference-key)
                                   (:new-key reference-key)
                                   (:prefix reference-key)))
          (make-id-key map
                       key-specification)
          (:reference-keys key-specification)))

(defn keys-to-kebab-case [a-map]
  (medley/map-keys camel-snake-kebab/->kebab-case-keyword
                   a-map))

(defn prepare [a-map key-specification]
  (-> a-map
      (keys-to-kebab-case)
      (make-id-keys key-specification)))

(def food-key-specification {:id-key :fdc-id
                             :prefix "food-"
                             :reference-keys [{:key :food-category-id
                                               :new-key :category
                                               :prefix "category-"}]})

#_(def food-nutrient-key-specification {:id-key (juxt :fdc-id :id)
                                        :discarded-keys [:id]
                                        :prefix "food-nutrient"
                                        :reference-keys [{:key :fdc-id
                                                          :prefix "food-"}
                                                         {:key :nutrient_id
                                                          :prefix "nutrient-"}]})


(def food-nutrient-key-specification {:id-key :id
                                      :prefix "food-nutrient-"
                                      :reference-keys [{:key :fdc-id
                                                        :new-key :food
                                                        :prefix "food-"}
                                                       {:key :nutrient_id
                                                        :new-key :nutrient
                                                        :prefix "nutrient-"}]})

(def nutrient-key-specification {:id-key :id
                                 :prefix "nutrient-"
                                 :reference-keys []})

(defn partial-right [function & arguments]
  (fn [argument]
    (apply function argument arguments)))

(def food-file-name "/Users/jukka/Documents/FoodData_Central_csv_2020-04-29/food.csv")
(def food-nutrient-file-name "/Users/jukka/Documents/FoodData_Central_csv_2020-04-29/food_nutrient.csv")
(def nutrient-file-name "/Users/jukka/Documents/FoodData_Central_csv_2020-04-29/nutrient.csv")

(defn reset-directory [path]
  (fs/delete-dir path)
  (fs/mkdirs path))

(defn reset-db []
  (def count-atom (atom 0))
  (fs/delete-dir "temp/db")
  (fs/mkdirs "temp/db")
  (def db-atom (atom (create-food-db "temp/db")))
  nil)

(def food-nutrient-sample [{:amount 0,
                            :min nil,
                            :fdc_id 2
                            :data_points nil,
                            :derivation_id 75,
                            :min_year_acquired nil,
                            :footnote nil,
                            :median nil,
                            :max nil,
                            :id 1
                            :nutrient_id 2}
                           {:amount 0,
                            :min nil,
                            :fdc_id 1
                            :data_points nil,
                            :derivation_id 75,
                            :min_year_acquired nil,
                            :footnote nil,
                            :median nil,
                            :max nil,
                            :id 1
                            :nutrient_id 1}])

(def nutrient-sample {:name "Nitrogen",
                      :unit_name "G",
                      :nutrient_nbr 202,
                      :rank 500,
                      :id 1})

(def food-sample {:data_type "agricultural_acquisition",
                  :description "Beans, Dry, Dark Red Kidney, 11F-8074 (0% moisture)",
                  :food_category_id "category-16",
                  :publication_date "2019-04-01",
                  :fdc_id 1})

(def food-nutrient-index-definitions [db-common/eav-index-definition
                                      #_(filter-by-attributes #{:name :description}
                                                              db-common/full-text-index-definition)

                                      #_(filter-by-attributes #{:food :amount}
                                                              db-common/avetc-index-definition)
                                      (db-common/composite-index-definition :text
                                                                            [:data_type
                                                                             {:attributes [:name :description]
                                                                              :value-funciton db-common/tokenize}])
                                      (db-common/composite-index-definition :data-type [:data_type])
                                      (db-common/composite-index-definition :food-nutrient-amount [:food :nutrient :amount])
                                      (db-common/composite-index-definition :nutrient-amount-food [:nutrient :amount :food])])
(comment
  (let [db-atom (atom (create-in-memory-db food-nutrient-index-definitions))]
    (transact-csv db-atom
                  food-nutrient-file-name
                  (comp (take 10)
                        (map (partial-right make-id-keys food-nutrient-key-specification))
                        (map (partial add-type :nutrient-amount-food))))
    (swap! db-atom
           db-common/transact
           #{["food-nutrient-3639112" :amount :set 2]})

    [#_(db-common/datoms-from @db-atom :avetc [:amount])
     #_(db-common/datoms-from @db-atom :eatcv [:amount])
     (db-common/datoms-from @db-atom :food-nutrient-amount [])
     (db-common/datoms-from @db-atom :nutrient-amount-food [])])

  (-> (atom (create-in-memory-db food-nutrient-index-definitions))
      (transact-with-transducer (map (partial-right make-id-keys food-nutrient-key-specification))
                                food-nutrient-sample)
      (swap! db-common/transact
             #{[["food-nutrient" [1 1]] :amount :set 3]})
      (deref)
      (db-common/datoms #_:eatcv :avetc [] #_[["food-nutrient" [1 1]]
                                              :amount]))

  (-> (create-in-memory-db food-nutrient-index-definitions)
      (db-common/transact #{[0 :amount :add 2]})
      (db-common/transact #{[0 :amount :set 3]})
      #_(db-common/value 0 :things)
      (db-common/datoms #_:eatcv :avtec nil))

  (time (def metadata-db-atom (let [path "temp/metadata"
                                    _ (reset-directory path)
                                    db-atom (atom (create-db path [db-common/eav-index-definition
                                                                   (filter-by-attributes #{:name :description}
                                                                                         db-common/full-text-index-definition)]))]
                                (transact-csv db-atom
                                              food-file-name
                                              (comp (take 1000)
                                                    (map (partial-right prepare food-key-specification))))

                                (transact-csv db-atom
                                              nutrient-file-name
                                              (comp (take 1000)
                                                    (map (partial-right prepare nutrient-key-specification))))
                                db-atom)))

  (take 100 (db-common/propositions-from-index (db-common/index @metadata-db-atom
                                                                :full-text)
                                               [:description "bean"]
                                               nil))

  (db-common/values @metadata-db-atom
                    "food-344744"
                    :description)

  (db-common/->Entity @metadata-db-atom
                      {#_:argupedia/questions #_{:multivalued? true
                                                 :reference? true}}
                      "food-344744")

  (time (def food-nutrient-db-atom (let [path "temp/food_nutrient"
                                         _ (reset-directory path)
                                         db-atom (atom (create-in-memory-db food-nutrient-index-definitions)
                                                       #_(create-db path food-nutrient-index-definitions))]
                                     (transact-csv db-atom
                                                   food-nutrient-file-name
                                                   (comp (take 1000)
                                                         (map (partial-right prepare food-nutrient-key-specification))))
                                     (transact-csv db-atom
                                                   nutrient-file-name
                                                   (comp (take 1000)
                                                         (map (partial-right prepare nutrient-key-specification))))

                                     (transact-csv db-atom
                                                   food-file-name
                                                   (comp (take 1000)
                                                         (map (partial-right prepare food-key-specification))))


                                     db-atom)))

  (count (transduce-csv nutrient-file-name
                        identity
                        :reducer conj))

  (defn entity [id]
    (db-common/->Entity @food-nutrient-db-atom
                        schema
                        id))

  (take 10 (remove (fn [food]
                     (= "branded_food" (:data_type food)))
                   (map entity (map last (db-common/propositions @food-nutrient-db-atom
                                                                 :full-text
                                                                 [:description "bean"])))))

  (:name (:nutrient (db-common/->Entity @food-nutrient-db-atom
                                        schema
                                        "food-nutrient-6232042")))

  (seq (db-common/->Entity @food-nutrient-db-atom
                           {:fdc_id {:reference? true}
                            :nutrient_id {:reference? true}}
                           "food-nutrient-6232042"))

  (:name (db-common/->Entity @food-nutrient-db-atom
                             {:fdc_id {:reference? true}
                              :nutrient_id {:reference? true}}
                             "nutrient-1293"))

  (take 100 (db-common/propositions @food-nutrient-db-atom
                                    :nutrient-amount-food
                                    ["nutrient-1293"]))

  (take 100 (db-common/propositions @food-nutrient-db-atom
                                    :eav
                                    ["nutrient-1293"]
                                    {:take-while-pattern-matches? false}))


  (transduce-csv food-file-name
                 (comp (csv-rows-to-maps :parse-value parse-number)
                       (map (partial-right make-id-keys food-key-specification))
                       (take 100))
                 :reducer conj)

  (transduce-csv food-nutrient-file-name
                 (comp (csv-rows-to-maps :parse-value (comp empty-string-to-nil
                                                            parse-number))
                       #_(remove (fn [food-nutrient]
                                   (= 0 (:amount food-nutrient))
                                   #_(= "" (:data_points food-nutrient))))
                       (take 100)
                       #_(map (partial-right make-id-keys food-nutrient-key-specification)))
                 :reducer conj)

  (transduce-csv nutrient-file-name
                 (comp (take 10)
                       (csv-rows-to-maps :parse-value parse-number)
                       (map (partial-right make-id-keys nutrient-key-specification)))
                 :reducer conj)

  @count-atom

  (db-common/datoms @db-atom
                    :description
                    [:description "beans"])

  (into #{} (map second (db-common/datoms @db-atom
                                          :avtec
                                          [:description])))


  (db-common/entities @db-atom
                      :description)

  (transduce-csv food-file-name
                 (comp (csv-rows-to-maps :parse-value parse-number)
                       #_(take 2))
                 :reducer (completing (fn [result entity]
                                        (conj result (:data_type entity))))
                 :initial-value #{})

  (time (def non-branded-foods (transduce-csv food-file-name
                                              (comp (csv-rows-to-maps parse-number)
                                                    (remove (fn [food]
                                                              (= "branded_food" (:data_type food))))
                                                    #_(take 2000))
                                              :reducer conj)))

  (count non-branded-foods)
  (take 10 non-branded-foods)

  (comp (csv-rows-to-maps parse-number)
        transducer
        (map map-to-transaction/maps-to-transaction)
        (map (partial db-common/transact db))
        (map (fn [_]
               (btree-db/store-index-roots-after-maximum-number-of-transactions db 1000))))

  (reset-db)

  (def initial-db @db-atom)

  (clojure.data/diff initial-db
                     @db-atom)

  (->> non-branded-foods
       (take 2)
       (map (partial-right
             make-id-keys
             food-key-specification))
       (map map-to-transaction/maps-to-transaction)
       (map (fn [transaction]
              #_(db-common/transact @db-atom transaction)
              (swap! db-atom db-common/transact transaction))))

  (future (time (transact-many! @db-atom
                                (fn []
                                  (transduce (comp (take 3000)
                                                   (map (fn [entity]
                                                          (swap! count-atom inc)
                                                          (map-to-transaction/maps-to-transaction (make-id-keys entity
                                                                                                                food-key-specification))))
                                                   (map (fn [transaction]
                                                          (swap! db-atom db-common/transact transaction)))
                                                   (map (fn [_]
                                                          (swap! db-atom btree-db/store-index-roots-after-maximum-number-of-transactions 10000))))
                                             (constantly nil)
                                             non-branded-foods)))))
  @count-atom
  @db-atom

  (map-to-transaction/maps-to-transaction ((partial-right make-id-keys food-key-specification) {:fdc_id 321506,
                                                                                                :data_type "sample_food",
                                                                                                :description "BEANS, SNAP, CANNED, DRAINED, DEL MONTE",
                                                                                                :food_category_id 11,
                                                                                                :publication_date "2019-04-01"}))

  (def food-nutrient (read-csv "/Users/jukka/Downloads/FoodData_Central_csv_2019-12-17/food_nutrient.csv"))


  (csv/transduce-maps "/Users/jukka/Downloads/FoodData_Central_csv_2019-12-17/food_nutrient.csv"
                      {:separator #","}
                      (comp (map (fn [entity-map]
                                   (prn entity-map)))
                            (take 1))
                      (constantly nil)
                      nil)


  data-csv/read-csv

  (with-open [reader (io/reader "/Users/jukka/Downloads/FoodData_Central_csv_2019-12-17/food_nutrient.csv")]
    (->> (data-csv/read-csv reader)
         (take 10)
         (csv-rows-to-maps)
         (map (partial medley/map-vals parse-number))
         (doall)))

  ;; TODO: remove-me
  )
