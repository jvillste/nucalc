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
            [camel-snake-kebab.core :as camel-snake-kebab]
            [argumentica.comparator :as comparator]
            [argumentica.btree-collection :as btree-collection]
            [nucalc.serialization :as serialization]
            [clj-time.core :as clj-time]
            [clj-time.format :as format]
            [clj-time.local :as local]
            [taoensso.tufte :as tufte]
            [clojure.core.reducers :as reducers])
  (:gen-class))

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
                                         (btree-collection/create-on-disk (str path "/" (name index-key))
                                                                          {:node-size 10001}))
                                       (file-transaction-log/create (str path "/transaction-log"))))

(defn create-in-memory-db [index-definitions]
  (db-common/db-from-index-definitions index-definitions
                                       (fn [index-key]
                                         #_(sorted-set-index/create)
                                         (btree-collection/create-in-memory)
                                         #_(btree-index/create-memory-btree-index 10001))
                                       (sorted-map-transaction-log/create)))

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
  (fn [rf]
    (let [keys (volatile! nil)]
      (fn
        ([result]
         (rf result))

        ([result row]
         (if (nil? @keys)
           (do (vreset! keys (map (comp keyword
                                        prepare-key-name)
                                  row))
               result)
           (rf result
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

(defn test-transducer [rf]
  (fn
    ([]
     (prn "initialize")
     (rf))

    ([result]
     (prn "finished" result)
     (rf result))

    ([result input]
     (prn "reduce" result input)
     (rf result input))))

(comment
  (transduce-csv "temp/sample.csv"
                 (comp (take 2)
                       test-transducer)
                 :reducer conj)

  (transduce-csv food-file-name
                 (comp (csv-rows-to-maps :parse-value parse-number)
                       (remove (fn [food]
                                 (= "branded_food" (:data_type food))))
                       (take 10))
                 :reducer conj)
  ) ;; TODO: remove-me

;; (defn prepare-transaction-log-for-batch [transaction-log]
;; )

(defn transact-many! [db transact]
  (transaction-log/make-transient! (:transaction-log db))

  (transact)

  (transaction-log/truncate! (:transaction-log db)
                             (inc (transaction-log/last-transaction-number (:transaction-log db))))
  (transaction-log/make-persistent! (:transaction-log db))
  nil)

(defn- finish-batch [state-atom batch-size entity-count db-atom]
  (let [elapsed-time (- (System/currentTimeMillis)
                        (:last-batch-ended @state-atom))]
    (println (java.util.Date.)
             (str (:count @state-atom) "/" entity-count)
             (str (int (* 100 (/ (:count @state-atom)
                                 entity-count)))
                  "%")
             (str "Indexing " batch-size " entities took " (/ (float elapsed-time)
                                                              1000) " seconds.")
             (str (float (/ elapsed-time
                            batch-size))
                  " milliseconds per entity.")
             (int (/ (* (/ (- entity-count
                              (:count @state-atom))
                           batch-size)
                        elapsed-time)
                     60
                     1000))
             "minutes remaining."))

  (when (instance? argumentica.btree_collection.BtreeCollection
                   (-> @db-atom :indexes vals first :collection))
    (swap! db-atom (fn [db]
                     (-> db
                         (btree-db/store-index-roots-after-maximum-number-of-transactions 0)
                         ;; TODO: store-index-roots-after-maximum-number-of-transactions already unloads all nodes but it would not need to
                         (btree-db/unload-nodes 20)
                         (btree-db/remove-old-roots)
                         (btree-db/collect-storage-garbage)))))

  (swap! state-atom assoc :last-batch-ended (System/currentTimeMillis)))

(defn batching-transducer [db-atom entity-count]
  (let [batch-size 500
        state-atom (atom {:count 0
                          :last-batch-ended (System/currentTimeMillis)})]
    (fn [rf]
      (fn
        ([] (rf))

        ([result]
         (finish-batch state-atom batch-size entity-count db-atom)
         (rf result))

        ([result input]
         (swap! state-atom update :count inc)
         (when (= 0
                  (mod (:count @state-atom)
                       batch-size))
           (finish-batch state-atom batch-size entity-count db-atom))
         (rf result input))))))

(comment
  (transduce batching-transducer conj [1 2 3])
  ) ;; TODO: remove-me


(def food-csv-rows-to-maps (csv-rows-to-maps :parse-value (comp empty-string-to-nil
                                                                parse-number)))

(defn make-transact-maps-transducer [db-atom transducer entity-count]
  (comp transducer
        (map map-to-transaction/maps-to-transaction)
        (map (fn [transaction]
               (swap! db-atom db-common/transact transaction)))
        (batching-transducer db-atom entity-count)))

(defn make-transact-csv-rows-transducer [db-atom transducer entity-count]
  (comp food-csv-rows-to-maps
        (make-transact-maps-transducer db-atom transducer entity-count)))

(defn line-count [file-name]
  (with-open [rdr (clojure.java.io/reader file-name)]
    (count (line-seq rdr))))


(defn transact-csv [db-atom file-name transducer]
  (println "loading" file-name)
  (transact-many! @db-atom
                  (fn []
                    (transduce-csv file-name
                                   (make-transact-csv-rows-transducer db-atom transducer (dec (line-count file-name)))))))

(defn transact-maps [db-atom maps transducer]
  (transact-many! @db-atom
                  (fn []
                    (transduce (make-transact-maps-transducer db-atom transducer (count maps))
                               (constantly nil)
                               maps))))


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
                                                         {:key :nutrient-id
                                                          :prefix "nutrient-"}]})


(def food-nutrient-key-specification {:id-key :id
                                      :prefix "food-nutrient-"
                                      :reference-keys [{:key :fdc-id
                                                        :new-key :food
                                                        :prefix "food-"}
                                                       {:key :nutrient-id
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


(comment
  [[:measurement? :nutrient :nutrient?]
   [:measurement? :amount :amount?]
   [:measurement? :food :food?]
   [:food? :data-type :data-type?]]

  [:measurement? [:nutrient :nutrient?
                  :amount :amount?
                  :food [:food? [:data-type :data-type?]]]]


  [:data-type? :nutrient? :amount? :food?]



  [:nutrient
   [:food :data-type]
   :amount
   :food]
  ) ;; TODO: remove-me


(defn load-csv [db-atom data-directory-path file-name transducer key-specification]
  (transact-csv db-atom
                (str data-directory-path "/" file-name)
                (comp (map (partial-right prepare key-specification))
                      transducer)))

(def data-types {:food {:file-name "food.csv"
                        :key-specification {:id-key :fdc-id
                                            :prefix "food-"
                                            :reference-keys [{:key :food-category-id
                                                              :new-key :category
                                                              :prefix "category-"}]}}

                 :nutrient {:file-name "nutrient.csv"
                            :key-specification {:id-key :id
                                                :prefix "nutrient-"
                                                :reference-keys []}}

                 :food-nutrient {:file-name "food_nutrient.csv"
                                 :key-specification {:id-key :id
                                                     :prefix "food-nutrient-"
                                                     :reference-keys [{:key :fdc-id
                                                                       :new-key :food
                                                                       :prefix "food-"}
                                                                      {:key :nutrient-id
                                                                       :new-key :nutrient
                                                                       :prefix "nutrient-"}]}}})

(defn load-file [db-atom data-type data-directory-path transducer]
  (load-csv db-atom
            data-directory-path
            (:file-name data-type)
            transducer
            (:key-specification data-type)))

(defn load-food-db! [db-atom data-directory-path row-limit]
  (let [transducer (if row-limit
                     (take row-limit)
                     identity)]
    (load-file db-atom (:food data-types) data-directory-path transducer)
    (load-file db-atom (:nutrient data-types) data-directory-path transducer)
    (load-file db-atom (:food-nutrient data-types) data-directory-path transducer
               #_(filter (fn [entity]
                           (if-let [food (:food entity)]
                             (contains? #{"food-344976"
                                          "food-345203"
                                          "food-345262"
                                          "food-345331"
                                          "food-345335"
                                          "food-345365"
                                          "food-345373"
                                          "food-345516"}
                                        food)
                             true))))
    db-atom))

(defn local-time-as-string []
  (format/unparse (format/formatter-local "HH:mm:ss")
                  (local/local-now)))

(defn write-csv-rows-to-data-file [csv-file-name output-file-name transducer]
  (serialization/with-data-output-stream output-file-name
    (fn [data-output-stream]
      (transduce-csv csv-file-name
                     (comp food-csv-rows-to-maps
                           transducer
                           (partition-all 500)
                           (map #(do (println "batch ready" (local-time-as-string))
                                     %))
                           (map #(serialization/write-to-data-output-stream data-output-stream %))))))
  (println "writing ready."))

(defn transact-data-file [db-atom file-name transducer key-specification entity-count]
  (transact-many! @db-atom
                  (fn []
                    (serialization/transduce-file file-name
                                                  :transducer (comp transducer
                                                                    (make-transact-maps-transducer db-atom
                                                                                                   (comp #_(take 10)
                                                                                                         (map (partial-right prepare key-specification)))
                                                                                                   entity-count))))))

(def non-branded-foods-file-name "temp/non-branded-foods.data")
(def non-branded-food-nutrient-file-name "temp/non-branded-food-nutrients.data")
(def non-branded-food-nutrient-sample-file-name "temp/non-branded-food-nutrient-sample.data")
(def non-branded-food-nutrient-sample-3000-file-name "temp/non-branded-food-nutrient-sample-3000.data")

(defn sample-indexes [db-atom]
  (for [index-key (keys (:indexes @db-atom))]
    [index-key (take 1 (db-common/propositions-from-index (db-common/index @db-atom index-key)
                                                          ::comparator/min
                                                          nil))]))

(tufte/add-basic-println-handler! {})

(comment
  (tufte/profile {} (Thread/sleep 100))
  ) ;; TODO: remove-me

;; todo: btree/next-sequence is called 30k times for each food!

;; #inst "2020-09-24T05:22:36.153-00:00" 500/35727 1% Indexing 500 entities took 7.301 seconds. 14.602 milliseconds per entity. 8 minutes remaining.
;; #inst "2020-09-24T05:22:56.400-00:00" 1000/35727 2% Indexing 500 entities took 20.208 seconds. 40.416 milliseconds per entity. 23 minutes remaining.
;; #inst "2020-09-24T05:23:29.444-00:00" 1500/35727 4% Indexing 500 entities took 32.979 seconds. 65.958 milliseconds per entity. 37 minutes remaining.
;; #inst "2020-09-24T05:24:13.014-00:00" 2000/35727 5% Indexing 500 entities took 43.481 seconds. 86.962 milliseconds per entity. 48 minutes remaining.
;; #inst "2020-09-24T05:25:03.642-00:00" 2500/35727 6% Indexing 500 entities took 50.532 seconds. 101.064 milliseconds per entity. 55 minutes remaining.
;; #inst "2020-09-24T05:26:07.861-00:00" 3000/35727 8% Indexing 500 entities took 64.099 seconds. 128.198 milliseconds per entity. 69 minutes remaining.
;; #inst "2020-09-24T05:26:17.927-00:00" 3036/35727 8% Indexing 500 entities took 9.91 seconds. 19.82 milliseconds per entity. 10 minutes remaining.
;; "Elapsed time: 229259.876068 msecs"
;; done

;; pId                     nCalls        Min      50% ≤      90% ≤      95% ≤      99% ≤        Max       Mean   MAD      Clock  Total

;; :all                         1     3.82m      3.82m      3.82m      3.82m      3.82m      3.82m      3.82m    ±0%     3.82m    100%
;; :next-sequence      92,245,824   102.00ns   228.00ns   370.00ns   461.00ns   673.00ns   245.95ms   816.83ns ±138%     1.26m     33%
;; :tufte/compaction          115   105.28ms   194.37ms   252.53ms   259.94ms   450.19ms   476.16ms   194.87ms  ±25%    22.41s     10%
;; :sequence-for-value     42,504     2.70μs    51.72μs    70.15μs    77.25μs   103.81μs   884.99ms   178.83μs ±142%     7.60s      3%

;; Accounted                                                                                                             5.58m    146%
;; Clock                                                                                                                 3.82m    100%


;; (def food-nutrient-index-definitions [db-common/eav-index-definition
;;                                       (db-common/composite-index-definition :data-type-text
;;                                                                             [:data-type
;;                                                                              {:attributes [:name :description]
;;                                                                               :value-function db-common/tokenize}])
;;                                       (db-common/enumeration-index-definition :data-type :data-type)
;;                                       (db-common/composite-index-definition :food-nutrient-amount [:food :nutrient :amount])
;;                                       (db-common/composite-index-definition :nutrient-amount-food [:nutrient :amount :food])
;;                                       ;; (db-common/rule-index-definition :datatype-nutrient-amount-food {:head [:?data-type :?nutrient :?amount :?food :?description]
;;                                       ;;                                                                  :body [[:eav
;;                                       ;;                                                                          [:?measurement :amount :?amount]
;;                                       ;;                                                                          [:?measurement :nutrient :?nutrient]
;;                                       ;;                                                                          [:?measurement :food :?food]
;;                                       ;;                                                                          [:?food :data-type :?data-type]
;;                                       ;;                                                                          [:?food :description :?description]]]})
;;                                       ])
;;
;; #inst "2020-09-25T04:14:09.476-00:00" 500/35727 1% Indexing 500 entities took 1.716 seconds. 3.432 milliseconds per entity. 2 minutes remaining.
;; #inst "2020-09-25T04:14:12.944-00:00" 1000/35727 2% Indexing 500 entities took 3.416 seconds. 6.832 milliseconds per entity. 3 minutes remaining.
;; #inst "2020-09-25T04:14:19.625-00:00" 1500/35727 4% Indexing 500 entities took 6.612 seconds. 13.224 milliseconds per entity. 7 minutes remaining.
;; #inst "2020-09-25T04:14:26.267-00:00" 2000/35727 5% Indexing 500 entities took 6.538 seconds. 13.076 milliseconds per entity. 7 minutes remaining.
;; #inst "2020-09-25T04:14:34.525-00:00" 2500/35727 6% Indexing 500 entities took 8.153 seconds. 16.306 milliseconds per entity. 9 minutes remaining.
;; #inst "2020-09-25T04:14:46.599-00:00" 3000/35727 8% Indexing 500 entities took 11.932 seconds. 23.864 milliseconds per entity. 13 minutes remaining.
;; #inst "2020-09-25T04:14:57.073-00:00" 3500/35727 9% Indexing 500 entities took 10.316 seconds. 20.632 milliseconds per entity. 11 minutes remaining.
;; #inst "2020-09-25T04:15:08.032-00:00" 4000/35727 11% Indexing 500 entities took 10.777 seconds. 21.554 milliseconds per entity. 11 minutes remaining.
;; #inst "2020-09-25T04:15:19.821-00:00" 4500/35727 12% Indexing 500 entities took 11.607 seconds. 23.214 milliseconds per entity. 12 minutes remaining.
;; #inst "2020-09-25T04:15:31.930-00:00" 5000/35727 13% Indexing 500 entities took 11.851 seconds. 23.702 milliseconds per entity. 12 minutes remaining.
;; #inst "2020-09-25T04:15:46.501-00:00" 5500/35727 15% Indexing 500 entities took 14.374 seconds. 28.748 milliseconds per entity. 14 minutes remaining.
;; #inst "2020-09-25T04:16:03.886-00:00" 6000/35727 16% Indexing 500 entities took 17.169 seconds. 34.338 milliseconds per entity. 17 minutes remaining.
;; #inst "2020-09-25T04:16:19.466-00:00" 6500/35727 18% Indexing 500 entities took 15.301 seconds. 30.602 milliseconds per entity. 14 minutes remaining.
;; #inst "2020-09-25T04:16:33.760-00:00" 7000/35727 19% Indexing 500 entities took 14.065 seconds. 28.13 milliseconds per entity. 13 minutes remaining.
;; #inst "2020-09-25T04:16:49.682-00:00" 7500/35727 20% Indexing 500 entities took 15.681 seconds. 31.362 milliseconds per entity. 14 minutes remaining.
;; #inst "2020-09-25T04:17:05.300-00:00" 8000/35727 22% Indexing 500 entities took 15.36 seconds. 30.72 milliseconds per entity. 14 minutes remaining.
;; #inst "2020-09-25T04:17:22.559-00:00" 8500/35727 23% Indexing 500 entities took 16.949 seconds. 33.898 milliseconds per entity. 15 minutes remaining.
;; #inst "2020-09-25T04:17:39.510-00:00" 9000/35727 25% Indexing 500 entities took 16.668 seconds. 33.336 milliseconds per entity. 14 minutes remaining.
;; #inst "2020-09-25T04:17:45.540-00:00" 9500/35727 26% Indexing 500 entities took 5.748 seconds. 11.496 milliseconds per entity. 5 minutes remaining.
;; #inst "2020-09-25T04:17:54.216-00:00" 10000/35727 27% Indexing 500 entities took 8.522 seconds. 17.044 milliseconds per entity. 7 minutes remaining.
;; #inst "2020-09-25T04:18:00.973-00:00" 10500/35727 29% Indexing 500 entities took 6.599 seconds. 13.198 milliseconds per entity. 5 minutes remaining.
;; #inst "2020-09-25T04:18:08.680-00:00" 11000/35727 30% Indexing 500 entities took 7.567 seconds. 15.134 milliseconds per entity. 6 minutes remaining.
;; #inst "2020-09-25T04:18:15.224-00:00" 11500/35727 32% Indexing 500 entities took 6.401 seconds. 12.802 milliseconds per entity. 5 minutes remaining.
;; #inst "2020-09-25T04:18:24.954-00:00" 12000/35727 33% Indexing 500 entities took 9.559 seconds. 19.118 milliseconds per entity. 7 minutes remaining.
;; #inst "2020-09-25T04:18:34.407-00:00" 12500/35727 34% Indexing 500 entities took 9.223 seconds. 18.446 milliseconds per entity. 7 minutes remaining.
;; #inst "2020-09-25T04:18:44.158-00:00" 13000/35727 36% Indexing 500 entities took 9.54 seconds. 19.08 milliseconds per entity. 7 minutes remaining.
;; #inst "2020-09-25T04:18:55.350-00:00" 13500/35727 37% Indexing 500 entities took 10.98 seconds. 21.96 milliseconds per entity. 8 minutes remaining.
;; #inst "2020-09-25T04:19:07.060-00:00" 14000/35727 39% Indexing 500 entities took 11.475 seconds. 22.95 milliseconds per entity. 8 minutes remaining.
;; #inst "2020-09-25T04:19:19.066-00:00" 14500/35727 40% Indexing 500 entities took 11.72 seconds. 23.44 milliseconds per entity. 8 minutes remaining.
;; #inst "2020-09-25T04:19:33.247-00:00" 15000/35727 41% Indexing 500 entities took 13.917 seconds. 27.834 milliseconds per entity. 9 minutes remaining.
;; #inst "2020-09-25T04:19:47.825-00:00" 15500/35727 43% Indexing 500 entities took 14.306 seconds. 28.612 milliseconds per entity. 9 minutes remaining.
;; #inst "2020-09-25T04:19:55.727-00:00" 16000/35727 44% Indexing 500 entities took 7.629 seconds. 15.258 milliseconds per entity. 5 minutes remaining.
;; #inst "2020-09-25T04:20:10.349-00:00" 16500/35727 46% Indexing 500 entities took 14.458 seconds. 28.916 milliseconds per entity. 9 minutes remaining.
;; #inst "2020-09-25T04:20:29.310-00:00" 17000/35727 47% Indexing 500 entities took 18.61 seconds. 37.22 milliseconds per entity. 11 minutes remaining.
;; #inst "2020-09-25T04:20:45.682-00:00" 17500/35727 48% Indexing 500 entities took 16.02 seconds. 32.04 milliseconds per entity. 9 minutes remaining.
;; #inst "2020-09-25T04:21:02.615-00:00" 18000/35727 50% Indexing 500 entities took 16.587 seconds. 33.174 milliseconds per entity. 9 minutes remaining.
;; #inst "2020-09-25T04:21:21.784-00:00" 18500/35727 51% Indexing 500 entities took 18.821 seconds. 37.642 milliseconds per entity. 10 minutes remaining.
;; #inst "2020-09-25T04:21:41.243-00:00" 19000/35727 53% Indexing 500 entities took 19.025 seconds. 38.05 milliseconds per entity. 10 minutes remaining.
;; #inst "2020-09-25T04:22:02.431-00:00" 19500/35727 54% Indexing 500 entities took 20.785 seconds. 41.57 milliseconds per entity. 11 minutes remaining.
;; #inst "2020-09-25T04:22:21.956-00:00" 20000/35727 55% Indexing 500 entities took 19.146 seconds. 38.292 milliseconds per entity. 10 minutes remaining.
;; #inst "2020-09-25T04:22:44.432-00:00" 20500/35727 57% Indexing 500 entities took 22.089 seconds. 44.178 milliseconds per entity. 11 minutes remaining.
;; #inst "2020-09-25T04:23:10.891-00:00" 21000/35727 58% Indexing 500 entities took 25.902 seconds. 51.804 milliseconds per entity. 12 minutes remaining.
;; #inst "2020-09-25T04:23:39.088-00:00" 21500/35727 60% Indexing 500 entities took 27.718 seconds. 55.436 milliseconds per entity. 13 minutes remaining.
;; #inst "2020-09-25T04:24:04.445-00:00" 22000/35727 61% Indexing 500 entities took 24.849 seconds. 49.698 milliseconds per entity. 11 minutes remaining.
;; #inst "2020-09-25T04:24:37.428-00:00" 22500/35727 62% Indexing 500 entities took 32.537 seconds. 65.074 milliseconds per entity. 14 minutes remaining.
;; #inst "2020-09-25T04:25:07.046-00:00" 23000/35727 64% Indexing 500 entities took 29.063 seconds. 58.126 milliseconds per entity. 12 minutes remaining.
;; #inst "2020-09-25T04:25:36.792-00:00" 23500/35727 65% Indexing 500 entities took 29.09 seconds. 58.18 milliseconds per entity. 11 minutes remaining.
;; #inst "2020-09-25T04:26:02.504-00:00" 24000/35727 67% Indexing 500 entities took 25.056 seconds. 50.112 milliseconds per entity. 9 minutes remaining.
;; #inst "2020-09-25T04:26:26.428-00:00" 24500/35727 68% Indexing 500 entities took 23.385 seconds. 46.77 milliseconds per entity. 8 minutes remaining.
;; #inst "2020-09-25T04:26:53.303-00:00" 25000/35727 69% Indexing 500 entities took 26.336 seconds. 52.672 milliseconds per entity. 9 minutes remaining.
;; #inst "2020-09-25T04:27:21.291-00:00" 25500/35727 71% Indexing 500 entities took 27.443 seconds. 54.886 milliseconds per entity. 9 minutes remaining.
;; #inst "2020-09-25T04:27:48.975-00:00" 26000/35727 72% Indexing 500 entities took 27.027 seconds. 54.054 milliseconds per entity. 8 minutes remaining.
;; #inst "2020-09-25T04:28:27.861-00:00" 26500/35727 74% Indexing 500 entities took 38.303 seconds. 76.606 milliseconds per entity. 11 minutes remaining.
;; #inst "2020-09-25T04:29:06.054-00:00" 27000/35727 75% Indexing 500 entities took 37.497 seconds. 74.994 milliseconds per entity. 10 minutes remaining.
;; Execution error (NoSuchFileException) at sun.nio.fs.UnixException/translateToIOException (UnixException.java:86).
;; temp/food_nutrient/data-type-text/metadata/E875BF763A999202972009ED48038220914FB55E92975B29AF19BD37C7240825


(def food-nutrient-index-definitions [db-common/eav-index-definition
                                      (db-common/composite-index-definition :data-type-text
                                                                            [:data-type
                                                                             {:attributes [:name :description]
                                                                              :value-function db-common/tokenize}])
                                      (db-common/enumeration-index-definition :data-type :data-type)
                                      (db-common/composite-index-definition :food-nutrient-amount [:food :nutrient :amount])
                                      (db-common/composite-index-definition :nutrient-amount-food [:nutrient :amount :food])
                                      ;; (db-common/rule-index-definition :datatype-nutrient-amount-food {:head [:?data-type :?nutrient :?amount :?food :?description]
                                      ;;                                                                  :body [[:eav
                                      ;;                                                                          [:?measurement :amount :?amount]
                                      ;;                                                                          [:?measurement :nutrient :?nutrient]
                                      ;;                                                                          [:?measurement :food :?food]
                                      ;;                                                                          [:?food :data-type :?data-type]
                                      ;;                                                                          [:?food :description :?description]]]})
                                      ])

(defn create-food-db [path]
  (atom #_(create-in-memory-db food-nutrient-index-definitions)
        (create-db path food-nutrient-index-definitions)))

(defn reset-db []
  (def count-atom (atom 0))
  (fs/delete-dir "temp/db")
  (fs/mkdirs "temp/db")
  (def db-atom (atom (create-food-db "temp/db")))
  nil)

(defn load-data-to-db [running?-atom]
  (time (def db-atom (try (tufte/profile {}
                                         (tufte/p :all
                                                  (let [path "temp/food_nutrient"
                                                        #_(reset-directory path)
                                                        db-atom (atom #_(create-in-memory-db food-nutrient-index-definitions
                                                                                             #_[db-common/eav-index-definition
                                                                                                (db-common/enumeration-index-definition :data-type :data-type)])
                                                                      (create-db path food-nutrient-index-definitions))]

                                                    #_(transact-data-file db-atom
                                                                        non-branded-foods-file-name
                                                                        #_(comp (drop 29000)
                                                                              (take-while (fn [_value] @running?-atom)))
                                                                        #_identity
                                                                        #_(take 3000)
                                                                        food-key-specification
                                                                        35727)
                                                    #_(transact-data-file db-atom
                                                                          #_non-branded-food-nutrient-file-name
                                                                          non-branded-food-nutrient-sample-file-name
                                                                          (take 4000)
                                                                          food-nutrient-key-specification
                                                                          1304201)

                                                    #_(transact-csv db-atom
                                                                    #_food-file-name
                                                                    "temp/food-sample"
                                                                    (comp (take 100)
                                                                          (map (partial-right prepare food-key-specification))))

                                                    #_(let [food-id-set (set (map first (db-common/datoms-from @db-atom
                                                                                                               :eav
                                                                                                               [nil :category])))])
                                                    #_(transact-csv db-atom
                                                                    food-nutrient-file-name
                                                                    (comp (take 100)
                                                                          (filter (fn [food-nutrient]
                                                                                    (contains? food-id-set
                                                                                               (str "food-" (:fdc_id)))))
                                                                          (map (partial-right prepare food-nutrient-key-specification))))

                                                    #_(transact-csv db-atom
                                                                    nutrient-file-name
                                                                    (comp (take 100)
                                                                          (map (partial-right prepare nutrient-key-specification))))


                                                    db-atom)))
                          (catch Exception e
                            (prn (Throwable->map e)))))))


(defn load-btree [root-node-id directory-storage]
  {:id root-node-id
   :children (for [child-id (:child-ids (btree/get-node-content directory-storage
                                                                root-node-id))]
               (load-btree child-id directory-storage))})
(comment

  (def running?-atom (atom true))
  (reset! running?-atom false)
  (def my-future (future (load-data-to-db running?-atom)
                         (println "done")))

  (:val @my-future)

  ;; #inst "2020-09-12T04:16:39.144-00:00" 500/35727 1% Indexing 500 entities took 0.519 seconds. 1.038 milliseconds per entity. 0 minutes remaining.
  ;; #inst "2020-09-12T04:16:39.879-00:00" 1000/35727 2% Indexing 500 entities took 0.7 seconds. 1.4 milliseconds per entity. 0 minutes remaining.
  ;; #inst "2020-09-12T04:16:40.914-00:00" 1500/35727 4% Indexing 500 entities took 0.982 seconds. 1.964 milliseconds per entity. 1 minutes remaining.
  ;; #inst "2020-09-12T04:16:42.222-00:00" 2000/35727 5% Indexing 500 entities took 1.274 seconds. 2.548 milliseconds per entity. 1 minutes remaining.
  ;; #inst "2020-09-12T04:16:44.611-00:00" 2500/35727 6% Indexing 500 entities took 2.346 seconds. 4.692 milliseconds per entity. 2 minutes remaining.
  ;; #inst "2020-09-12T04:16:46.179-00:00" 3000/35727 8% Indexing 500 entities took 1.502 seconds. 3.004 milliseconds per entity. 1 minutes remaining.
  ;; #inst "2020-09-12T04:16:48.784-00:00" 3500/35727 9% Indexing 500 entities took 2.559 seconds. 5.118 milliseconds per entity. 2 minutes remaining.
  ;; #inst "2020-09-12T04:16:50.484-00:00" 4000/35727 11% Indexing 500 entities took 1.639 seconds. 3.278 milliseconds per entity. 1 minutes remaining.
  ;; #inst "2020-09-12T04:16:53.129-00:00" 4500/35727 12% Indexing 500 entities took 2.587 seconds. 5.174 milliseconds per entity. 2 minutes remaining.
  ;; #inst "2020-09-12T04:16:54.885-00:00" 5000/35727 13% Indexing 500 entities took 1.699 seconds. 3.398 milliseconds per entity. 1 minutes remaining.
  ;; #inst "2020-09-12T04:16:54.934-00:00" 5000/35727 13% Indexing 500 entities took 0.0 seconds. 0.0 milliseconds per entity. 0 minutes remaining.


  ;; "Elapsed time: 16341.678315 msecs"
  ;; #'nucalc.core/db-atom

  (future-cancel my-future)
  (sample-indexes db-atom)

  ;; non branded foods


  (def writing-future (future (write-csv-rows-to-data-file food-file-name
                                                           non-branded-foods-file-name
                                                           (filter (fn [food]
                                                                     (not (= "branded_food"
                                                                             (:data_type food))))))))

  (def non-branded-food-id-set (into #{}
                                     (comp (take 3000)
                                           (map :fdc_id))
                                     (serialization/file-reducible non-branded-foods-file-name)))

  (count non-branded-food-id-set)
  (take 10 non-branded-food-id-set)

  ;; non branded food-nutrients


  (def writing-future (future (write-csv-rows-to-data-file food-nutrient-file-name
                                                           ;; non-branded-food-nutrient-file-name
                                                           non-branded-food-nutrient-sample-3000-file-name
                                                           (filter (fn [food-nutrient]
                                                                     (contains? non-branded-food-id-set
                                                                                (:fdc_id food-nutrient)))))))


  (serialization/transduce-file non-branded-food-nutrient-file-name
                                :initial-value 0
                                :reducer (completing (fn [count item_]
                                                       (inc count))))

  (into []
        (take 10)
        (serialization/file-reducible ;; non-branded-food-nutrient-file-name
         non-branded-food-nutrient-sample-3000-file-name))

  (net.cgrand.xforms/count identity
                           (serialization/file-reducible
                            #_non-branded-food-nutrient-file-name
                            non-branded-food-nutrient-sample-file-name
                            #_non-branded-food-nutrient-sample-3000-file-name))

  (realized? writing-future)






  (common/values-from-enumeration-index (common/index @db-atom
                                                      :data-type)
                                        nil)

  (keys (:indexes @db-atom))
  ;; => (:eav :data-type :food-nutrient-amount :nutrient-amount-food)

  (into []
        (take 100)
        (db-common/datoms-from @db-atom
                                :data-type
                               ;; :food-nutrient-amount
                               ;; :eav
                               []))



  (take 100 (db-common/propositions @db-atom
                                    :eav
                                    [::comparator/min]
                                    {:take-while-pattern-matches? false}))

  (take 100 (db-common/propositions @db-atom
                                    :eav
                                    [::comparator/min]
                                    {:take-while-pattern-matches? false}))

  (db-common/transduce-propositions (common/collection @db-atom :eav)
                                    ["food-749289"]
                                    {:transducer (take 10)
                                     :reducer conj
                                     ;;                                     :take-while-pattern-matches? false
                                     })





  (def db-atom (let [path "temp/food_nutrient"
                     _ (reset-directory path)
                     db-atom (create-food-db path)]
                 (load-food-db! db-atom
                                "/Users/jukka/Documents/FoodData_Central_csv_2020-04-29"
                                300)
                 db-atom))

  [(db-common/datoms-from @db-atom :eav [])
   (db-common/datoms-from @db-atom :data-type-text [])
   (db-common/datoms-from @db-atom :food-nutrient-amount [])
   (db-common/datoms-from @db-atom :nutrient-amount-food [])]

  (transduce-csv food-nutrient-file-name
                 (comp food-csv-rows-to-maps
                       (take 1)
                       #_(map (partial-right prepare food-nutrient-key-specification)))
                 :reducer conj)

  (storage/get-edn-from-storage! (directory-storage/create "temp/food_nutrient/data-type-text/metadata")
                                 "roots")
  (storage/get-edn-from-storage! (directory-storage/create "temp/food_nutrient/data-type-text/metadata")
                                 "F14B3A0514F2EE1E1542F759ED5A62AD869BDAC3724EF85E406D299BB4D44577")

  (storage/get-edn-from-storage! (directory-storage/create "temp/food_nutrient/data-type-text/nodes")
                                 "F14B3A0514F2EE1E1542F759ED5A62AD869BDAC3724EF85E406D299BB4D44577")

  (storage/get-edn-from-storage! (directory-storage/create "temp/food_nutrient/food-nutrient/nodes")
                                 "2972C6C9818042859708646AAE63EE7DD8674F50918052FA361D91FDC8F3951F")

  (load-btree "D177932C2D5E2E9E3EF3076D96D5DFB8B39243E169C54D9409987E11784F5C09"
              (directory-storage/create "temp/food_nutrient/data-type-text/metadata"))

  (def node-content (time (btree/get-node-content (directory-storage/create "temp/food_nutrient/data-type-text/nodes")
                                                  "F5F4B444990B5AB08FC01D61EC32DC064BBE0E8918326B5DE794D833C026EE81")))
  (count (:values  node-content))
  (take 10 (:values  node-content))

  (storage/get-edn-from-storage! (directory-storage/create "temp/food_nutrient/data-type-text/metadata")
                                 #_"FF6AC77E4B0A0BDA5645C5985AE314FB60306ADD1F138F156A811A263767C168"
                                 "D177932C2D5E2E9E3EF3076D96D5DFB8B39243E169C54D9409987E11784F5C09"
                                 #_"roots")

  (storage/get-from-storage! (directory-storage/create "temp/food_nutrient/data-type-text/metadata")
                             "roots3")

  (let [db-atom (atom (create-in-memory-db food-nutrient-index-definitions))]
    (transact-csv db-atom
                  food-nutrient-file-name
                  (comp (take 10)
                        (map (partial-right make-id-keys food-nutrient-key-specification))
                        (map (partial add-type :nutrient-amount-food))))
    (swap! db-atom
           db-common/transact
           #{["food-nutrient-3639112" :amount :set 2]})

    )



  (-> @db-atom
      :indexes :food-nutrient-amount)

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
                      "food-748588")

  (defn entity [id]
    (db-common/->Entity @db-atom
                        schema
                        id))

  (take 10 (remove (fn [food]
                     (= "branded_food" (:data_type food)))
                   (map entity (map last (db-common/propositions @db-atom
                                                                 :full-text
                                                                 [:description "bean"])))))

  (db-common/->Entity (db-common/deref @db-atom)
                      schema
                      "food-748588")

  (seq (db-common/->Entity @db-atom
                           {:fdc_id {:reference? true}
                            :nutrient_id {:reference? true}}
                           "food-nutrient-6232042"))

  (:name (db-common/->Entity @db-atom
                             {:fdc_id {:reference? true}
                              :nutrient_id {:reference? true}}
                             "nutrient-1293"))

  (-> @db-atom
      :indexes
      :data-type-text)



  (defn query-foods [data-type query]
    (take 10 (map last (db-common/propositions @db-atom
                                               :data-type-text
                                               [data-type query]))))

  (map :dali/id #_:description (map entity (query-foods "branded_food" "potato")))

  (common/values-from-enumeration @db-atom
                                  :data-type)

  (take 100 (db-common/propositions @db-atom
                                    :nutrient-amount-food
                                    []))

  (take 100 (db-common/propositions @db-atom
                                    :nutrient-amount-food
                                    ["nutrient-1293"]))

  (take 100 (db-common/propositions @db-atom
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

(defn -main [& arguments]
  (println "moi"))
