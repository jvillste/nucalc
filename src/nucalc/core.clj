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
            [medley.core :as medley]))

(defn full-text-index [attribute]
  {:key attribute
   :eatcv-to-datoms (fn [indexes e a t c v]
                      (if (= a attribute)
                        (db-common/eatcv-to-full-text-avtec db-common/tokenize
                                                            indexes e a t c v)
                        []))
   :datom-transaction-number-index 2})

(defn create-db [path]
  (db-common/db-from-index-definitions (conj db-common/base-index-definitions
                                             (full-text-index :description))
                                       (fn [index-key]
                                         (btree-index/create-directory-btree-index (str path "/" (name index-key))
                                                                                   1001))
                                       (file-transaction-log/create (str path "/transaction-log"))))


(def schema
  {:argupedia/questions {:multivalued? true
                         :reference? true}
   :argupedia/answers {:multivalued? true
                       :reference? true}
   :argupedia/pros {:multivalued? true
                    :reference? true}
   :argupedia/cons {:multivalued? true
                    :reference? true}})

(defn parse-number [string]
  (try
    (Integer/parseInt string)
    (catch Exception e
      (try
        (Double/parseDouble string)
        (catch Exception e
          string)))))

(defn csv-rows-to-maps
  ([]
   (csv-rows-to-maps identity))
  ([parse-value]
   (fn [reducing-function]
     (let [keys (volatile! nil)]
       (completing (fn [result row]
                     (if (nil? @keys)
                       (do (vreset! keys (map keyword row))
                           result)
                       (reducing-function result
                                          (zipmap @keys (map parse-value row))))))))))

(deftest test-csv-rows-to-maps
  (is (= [{:a 1, :b 2}]
         (transduce (comp (csv-rows-to-maps (fn [value] (Integer/parseInt value)))
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

(comment
  (apply concat {:a {1 2} :c :d})
  ) ;; TODO: remove-me

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

#_(defn transduce-csv
    ([file-name transducer & {:as options}]
     (let [{:keys [reducer initial-value]} (merge {:reducer (constantly nil)
                                                   :initial-value nil
                                                   :separator ","
                                                   :quote "\""}
                                                  options)]
       (with-open [reader (io/reader file-name)]
         (let [reducing-function (transducer reducer)]
           (loop [rows (data-csv/read-csv reader (select-keys options [:separator :quote]))
                  value initial-value]
             (if-let [line (first rows)]
               (let [result (reducing-function value
                                               line)]
                 (if (reduced? result)
                   (do (reducing-function @result))
                   (recur (rest rows)
                          result)))
               (reducing-function value))))))))

(comment
  (transduce-csv "temp/sample.csv"
                 (take 2)
                 :reducer conj)

  (transduce-csv "/Users/jukka/Downloads/FoodData_Central_csv_2019-12-17/food.csv"
                 (comp (csv-rows-to-maps parse-number)
                       (remove (fn [food]
                                 (= "branded_food" (:data_type food))))
                       (take 10))
                 :reducer conj)
  ) ;; TODO: remove-me

(defn transact-many! [db transact]
  (transaction-log/make-transient! (:transaction-log db))
  (transact)
  (btree-db/store-index-roots-after-maximum-number-of-transactions db 0)
  (transaction-log/truncate! (:transaction-log db)
                             (inc (transaction-log/last-transaction-number (:transaction-log db))))
  (transaction-log/make-persistent! (:transaction-log db))
  nil)

(defn make-transact-transducer [db transducer]
  (comp (csv-rows-to-maps parse-number)
        transducer
        (map map-to-transaction/maps-to-transaction)
        (map (partial db-common/transact db))
        (map (fn [_]
               (btree-db/store-index-roots-after-maximum-number-of-transactions db 1000)))))

(defn transact-csv [db file-name transducer]
  (transact-many! (fn []
                    (transduce-csv file-name
                                   (make-transact-transducer db transducer)))))

#_(defn read-csv [file-name]
    (with-open [reader (io/reader file-name)]
      (doall (csv-rows-to-maps (csv/read-csv reader)))))

(defn make-id-key [map key prefix]
  (-> (assoc map :dali/id (str prefix (key map)))
      (dissoc key)))

(defn make-reference-id-key [map key prefix]
  (assoc map key (str prefix (key map))))


(defn make-id-keys [map key-specification]
  (reduce (fn [map reference-key]
            (make-reference-id-key map
                                   (:key reference-key)
                                   (:prefix reference-key)))
          (make-id-key map
                       (:id-key key-specification)
                       (:prefix key-specification))
          (:reference-keys key-specification)))

(def food-key-specification {:id-key :fdc_id
                             :prefix "food-"
                             :reference-keys [{:key :food_category_id
                                               :prefix "category-"}]})

(defn partial-right [function & arguments]
  (fn [argument]
    (apply function argument arguments)))

(def food-file-name "/Users/jukka/Downloads/FoodData_Central_csv_2019-12-17/food.csv")

(comment


  (future (do
            (def count-atom (atom 0))
            (fs/delete-dir "temp/db")
            (fs/mkdirs "temp/db")

            (def db-atom (atom (create-db "temp/db")))

            #_(swap! db-atom (partial-right transact-csv
                                          food-file-name
                                          (comp (remove (fn [food]
                                                          (= "branded_food" (:data_type food))))
                                                (take 200000)
                                                (map (fn [entity]
                                                       (swap! count-atom inc)
                                                       entity))
                                                (map (partial-right make-id-keys food-key-specification)))))
            nil))

  @count-atom

  (db-common/datoms @db-atom
                    :description
                    [:description "beans"])

  (into #{} (map second (db-common/datoms @db-atom
                                          :avtec
                                          [:description])))


  (db-common/entities @db-atom
                      :description
                      )

  (transduce-csv food-file-name
                 (comp (csv-rows-to-maps parse-number)
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

  (time (transact-many! @db-atom
                        (fn []
                          (transduce (comp #_(take 1000)
                                           (map map-to-transaction/maps-to-transaction)
                                           (map (fn [transaction]
                                                  (swap! db-atom db-common/transact transaction)))
                                           (map (fn [_]
                                                  (swap! db-atom btree-db/store-index-roots-after-maximum-number-of-transactions 10000))))
                                     (constantly nil)
                                     non-branded-foods))))

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
