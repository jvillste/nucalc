(ns nucalc.core
  (:require [argumentica.btree-index :as btree-index]
            [argumentica.mutable-collection :as mutable-collection]
            [argumentica.db.file-transaction-log :as file-transaction-log]
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
            [argumentica.reduction :as reduction]
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
            [clojure.core.reducers :as reducers]
            [argumentica.db.query :as query]
            [argumentica.node-serialization :as node-serialization]
            [argumentica.sorted-reducible :as sorted-reducible]
            [argumentica.db.multifile-transaction-log :as multifile-transaction-log]
            [nucalc.csv :as nucalc-csv]
            [argumentica.util :as util]
            [net.cgrand.xforms :as xforms]
            [argumentica.leaf-node-serialization :as leaf-node-serialization]
            [clojure.edn :as edn])
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

(defn create-transaction-log [directory-path]
  (fs/mkdirs directory-path)
  (multifile-transaction-log/open directory-path))

(defn create-db [path index-definitions]
  (db-common/db-from-index-definitions index-definitions
                                       (fn [index-key]
                                         (btree-collection/create-on-disk (str path "/" (name index-key))
                                                                          {:node-size 10001}))
                                       (do (fs/mkdirs (str path "/transaction-log"))
                                           (multifile-transaction-log/open (str path "/transaction-log")))))

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
                             (transaction-log/last-transaction-number (:transaction-log db)))
  (transaction-log/make-persistent! (:transaction-log db))
  nil)

(defn- clean-up-after-indexing [db]
  (-> db
      (btree-db/store-index-roots-after-maximum-number-of-transactions 0)
      (btree-db/unload-nodes 200)
      (btree-db/remove-old-roots)
      (btree-db/collect-storage-garbage)))

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
    (swap! db-atom clean-up-after-indexing))

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


(def food-csv-rows-to-maps (csv-rows-to-maps :parse-value (comp empty-string-to-nil
                                                                parse-number)))

(defn make-transact-maps-transducer [db-atom transducer entity-count]
  (comp transducer
        (map #(medley/remove-vals nil? %))
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
  (transduce-csv file-name
                 (make-transact-csv-rows-transducer db-atom transducer (dec (line-count file-name)))))

(defn transact-maps [db-atom maps transducer]
  (transduce (make-transact-maps-transducer db-atom transducer (count maps))
             (constantly nil)
             maps))


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
  (-> map
      (assoc new-key (str prefix (key map)))
      (dissoc key)))

(defn add-type [type a-map]
  (assoc a-map :type type))

(defn make-id-keys [key-specification map]
  (reduce (fn [map reference-key]
            (make-reference-id-key map
                                   (:key reference-key)
                                   (:new-key reference-key)
                                   (:prefix reference-key)))
          (make-id-key map
                       key-specification)
          (:reference-keys key-specification)))

(deftest test-make-id-keys
  (is (= {:dali/id "thing-1"
          :other-thing "other-thing-2"}
         (make-id-keys {:id-key :id
                        :prefix "thing-"
                        :reference-keys [{:key :other-thing-id
                                          :new-key :other-thing
                                          :prefix "other-thing-"}]}
                       {:id 1
                        :other-thing-id 2}))))

(defn keys-to-kebab-case [a-map]
  (medley/map-keys camel-snake-kebab/->kebab-case-keyword
                   a-map))

(defn prepare [a-map key-specification]
  (->> a-map
       (keys-to-kebab-case)
       (make-id-keys)))

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

#_(defn write-csv-rows-to-data-file [csv-file-name output-file-name transducer]
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

(defn write-csv-rows-to-data-file [csv-file-name output-file-name transducer]
  (let [total-count (reduce reduction/count-inputs (nucalc-csv/line-reducible (io/reader csv-file-name)))]
    (println "total count is" total-count)
    (serialization/with-data-output-stream output-file-name
      (fn [data-output-stream]
        (reduction/process! (nucalc-csv/hashmap-reducible-from-csv (io/reader csv-file-name)
                                                                   {:value-function parse-number})
                            (reduction/report-progress-every-n-seconds 5
                                                                       (reduction/handle-batch-ending-by-printing-report (str "\n" csv-file-name " -> " output-file-name)
                                                                                                                         total-count))
                            transducer
                            (partition-all 500)
                            (map #(serialization/write-to-data-output-stream data-output-stream %))))))
  (println "writing ready."))

(defn transact-data-file [db-atom file-name transducer key-specification entity-count]
  (serialization/transduce-file file-name
                                :transducer (comp transducer
                                                  (make-transact-maps-transducer db-atom
                                                                                 (comp #_(take 10)
                                                                                       (map (partial-right prepare key-specification)))
                                                                                 entity-count))))

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

(def food-nutrient-index-definitions [db-common/eav-index-definition
                                      ;; (db-common/composite-index-definition :text
                                      ;;                                       [{:attributes [:name :description]
                                      ;;                                         :value-function db-common/tokenize}])
                                      ;; (db-common/enumeration-index-definition :data-type :data-type)
                                      ;; (db-common/composite-index-definition :food-nutrient-amount [:food :nutrient :amount])
                                      ;; (db-common/composite-index-definition :nutrient-amount-food [:nutrient :amount :food])
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

(def db-path "temp/food_nutrient")

(defn load-file-to-db [db-path file-name key-specification running?-atom]
  (let [db-atom (atom (create-db db-path food-nutrient-index-definitions))]

    (transact-csv db-atom
                  file-name
                  (map (partial-right prepare nutrient-key-specification)))

    #_(transact-data-file db-atom
                          non-branded-foods-file-name
                          #_(comp (drop 29000)
                                  (take-while (fn [_value] @running?-atom)))
                          identity
                          #_(take 3000)
                          food-key-specification
                          35727)
    (transact-data-file db-atom
                        non-branded-food-nutrient-file-name
                        #_non-branded-food-nutrient-sample-file-name
                        #_non-branded-food-nutrient-sample-3000-file-name
                        (take-while (fn [_value] @running?-atom))
                        #_(take 3000)
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


    db-atom))

(defn load-data-to-db [running?-atom]
  (time (def db-atom (try (tufte/profile {}
                                         (tufte/p :all
                                                  (let [path db-path
                                                        _ (reset-directory path)
                                                        db-atom (atom #_(create-in-memory-db food-nutrient-index-definitions
                                                                                             #_[db-common/eav-index-definition
                                                                                                (db-common/enumeration-index-definition :data-type :data-type)])
                                                                      (create-db path food-nutrient-index-definitions))]

                                                    #_(transact-csv db-atom
                                                                    nutrient-file-name
                                                                    (map (partial-right prepare nutrient-key-specification)))

                                                    (transact-data-file db-atom
                                                                        non-branded-foods-file-name
                                                                        #_(comp (drop 29000)
                                                                                (take-while (fn [_value] @running?-atom)))
                                                                        #_identity
                                                                        (take 5000)
                                                                        food-key-specification
                                                                        35727)

                                                    #_(transact-data-file db-atom
                                                                          non-branded-food-nutrient-file-name
                                                                          #_non-branded-food-nutrient-sample-file-name
                                                                          #_non-branded-food-nutrient-sample-3000-file-name
                                                                          (take-while (fn [_value] @running?-atom))
                                                                          #_(take 3000)
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


                                                    db-atom)))
                          (catch Exception e
                            (prn (Throwable->map e)))))))


(defn load-btree [root-node-id directory-storage]
  {:id root-node-id
   :children (for [child-id (:child-ids (btree/get-node-content directory-storage
                                                                root-node-id))]
               (load-btree child-id directory-storage))})

(defn load-btree-2 [storage-key directory-storage]
  {:storage-key storage-key
   :children (for [child-storage-key (map :storage-key (:children (btree/load-node-data directory-storage
                                                                                        storage-key)))]
               (load-btree-2 child-storage-key directory-storage))})
(defonce running?-atom (atom true))

;;TODO: (load-data-to-db running?-atom) does not work

(defn write-to-btree [])

(defn latest-root-metadata [btree-collection]
  (->> btree-collection
       btree-collection/btree
       btree/roots-2
       btree/latest-of-roots
       :metadata))

#_(defn write-batch-to-btree-collection! [reducible batch-size entity-to-rows btree-collection]
    (let [first-input-number (inc (or (:last-input-number (latest-root-metadata btree-collection))
                                      -1))
          input-count (transduce (comp (drop (inc first-input-number))
                                       (take batch-size)
                                       (reduction/report-progress-every-n-seconds 1
                                                                                  (reduction/handle-batch-ending-by-printing-report "\n"
                                                                                                                                    batch-size))
                                       (map (fn [entity]
                                              (run! (partial mutable-collection/add! btree-collection)
                                                    (entity-to-rows entity))
                                              entity)))
                                 count-inputs
                                 reducible)]

      (btree-collection/locking-apply-to-btree! btree-collection
                                                btree/store-root-2
                                                {:last-input-number (+ first-input-number input-count)})

      ;; (btree-collection/locking-apply-to-btree! btree-collection
      ;;                                           btree/remove-old-roots-2)

      ;; (btree-collection/locking-apply-to-btree! btree-collection
      ;;                                           btree/collect-storage-garbage)
      nil))

(defn recreate-directory [path]
  (fs/delete-dir path)
  (fs/mkdirs path)
  path)

(comment
  (count (node-serialization/serialize {:values (range 10)}))
  ) ;; TODO: remove-me

(defn write-batches-to-btree-collection! [reducible total-count value-to-rows node-size btree-path]
  (println "Starting writing.")
  (println "btree path:" btree-path)
  (println "node size:" node-size)

  (let [start-time (System/currentTimeMillis)
        run?-atom (atom true)]
    (future (try (let [btree-collection (-> (btree-collection/create-on-disk btree-path
                                                                             {:node-size node-size})
                                            (btree-collection/locking-apply-to-btree! btree/make-transient))
                       first-input-number (inc (or (:last-input-number (latest-root-metadata btree-collection))
                                                   -1))
                       input-count-atom (atom 0)
                       finish-batch (fn [_input-count]
                                      (println "used time in seconds" (float (/ (- (System/currentTimeMillis)
                                                                                   start-time)
                                                                                1000)))
                                      (println "writing root")

                                      (btree-collection/locking-apply-to-btree! btree-collection
                                                                                (fn [btree]
                                                                                  (-> btree
                                                                                      (btree/store-root-2 {:last-input-number (+ first-input-number
                                                                                                                                 @input-count-atom)})
                                                                                      btree/remove-old-roots-2
                                                                                      btree/collect-storage-garbage)))
                                      (println "writing root done"))]
                   (println "starting from" first-input-number)
                   (transduce (comp (drop (inc first-input-number))
                                    (reduction/report-progress-every-n-seconds 5
                                                                               (reduction/handle-batch-ending-by-printing-report (str "\n" btree-path)
                                                                                                                                 total-count))
                                    (reduction/run-every-n-seconds (* 5 60) finish-batch)
                                    (reduction/run-while-true run?-atom)
                                    (map (fn [entity]
                                           (run! (partial mutable-collection/add! btree-collection)
                                                 (value-to-rows entity))
                                           (swap! input-count-atom inc)
                                           entity)))
                              reduction/count-inputs
                              reducible)
                   (btree-collection/locking-apply-to-btree! btree-collection
                                                             btree/make-persistent!)
                   (println "finished writing"))
                 (catch Exception e
                   (prn e))))
    run?-atom))

(defn bytes-per-value [value-to-rows reducible]
  (let [values (into []
                     (take 1000)
                     reducible)]
    (int (/ (count (node-serialization/serialize {:values (mapcat value-to-rows values)}))
            (count values)))))

(deftest test-bytes-per-value
  (is (= 5
         (bytes-per-value vector (range 1000)))))
(comment
  (->> (range 10)
       (map (fn [number]
              (.limit (clojure.data.fressian/write number))))
       (reduce +))
  (count (node-serialization/serialize {:values (range 20)}))
  (count (node-serialization/with-data-output-stream
           (fn [data-output-stream]
             (node-serialization/write-rows (range 10)
                                            data-output-stream))))

  (type (node-serialization/serialize {:values (range 1000)}))
  (.limit (clojure.data.fressian/write 1000))
  )


(defn write-batches-to-btree-collection2! [reducible value-to-rows btree-path]
  (write-batches-to-btree-collection! reducible
                                      (reduce reduction/count-inputs 0 reducible)
                                      value-to-rows
                                      (quot 600000
                                            (bytes-per-value value-to-rows reducible))
                                      btree-path))

(defn food-to-row [food]
  [((juxt :fdc-id
          :description
          :data-type
          :food-category-id)
    food)])

(deftest test-food-to-row
  (is (= [["344604"
           "Tutturosso Green 14.5oz. NSA Italian Diced Tomatoes"
           "branded_food"
           ""]]
         (food-to-row {:fdc-id "344604",
                       :data-type "branded_food",
                       :description "Tutturosso Green 14.5oz. NSA Italian Diced Tomatoes",
                       :food-category-id "",
                       :publication-date "2019-04-01"}))))

(defn create-btree [directory-path value-to-rows column-keys reducible]
  (recreate-directory directory-path)
  (fs/mkdirs (str directory-path "/metadata"))
  (spit (str directory-path "/metadata/keys")
        (pr-str column-keys))
  (write-batches-to-btree-collection2! reducible
                                       value-to-rows
                                       directory-path))

(defn create-projection-btree [directory-path column-keys reducible]
  (create-btree directory-path
                (fn [value]
                  [((apply juxt column-keys) value)])
                column-keys
                reducible))

(defn- full-text-rows [text-column-keys other-column-keys value]
  (for [token (->> text-column-keys
                   (select-keys value)
                   vals
                   (string/join " ")
                   db-common/tokenize
                   distinct)]
    (into [token]
          ((apply juxt other-column-keys) value))))

(deftest test-full-text-rows
  (is (= (["this" 123 "this is description"]
          ["is" 123 "this is description"]
          ["description" 123 "this is description"]
          ["title" 123 "this is description"])
         (full-text-rows [:description :title]
                         [:id :description]
                         {:description "this is description"
                          :title "this is title"
                          :id 123}))))

(defn create-full-text-btree [directory-path text-column-keys other-column-keys reducible]
  (create-btree directory-path
                (partial full-text-rows text-column-keys other-column-keys)
                (concat [:token] other-column-keys)
                reducible))

(defn btree-keys [btree-path]
  (edn/read (java.io.PushbackReader. (io/reader (str btree-path "/metadata/keys")))))

(defn rows-to-maps [keys]
  (map (fn [row]
         (into {} (map vector keys row)))))

(defn open-btree [btree-path]
  (assoc (btree-collection/create-on-disk btree-path)
         :keys (btree-keys btree-path)))

(defn btree-subreducible [btree pattern]
  (eduction (rows-to-maps (:keys btree))
            (sorted-reducible/subreducible btree
                                           (or pattern
                                               ::comparator/min))))

(defn btree-matching-subreducible [btree pattern]
  (eduction (comp (db-common/take-while-pattern-matches pattern)
                  (rows-to-maps (:keys btree)))
            (sorted-reducible/subreducible btree
                                           (or pattern
                                               ::comparator/min))))

(comment
  (transduce identity
             reduction/count-inputs
             (range 3))

  (reduce reduction/count-inputs
          0
          (range 3))

  (def transaction-log-path "temp/test-transaction-log")
  (fs/mkdirs transaction-log-path)

  (def foods-path "temp/test-foods")
  (do (fs/delete-dir foods-path)
      (fs/mkdirs foods-path))

  (def full-text-path "temp/test-full-text")






























  ;; print 5 lines from the CSV file

  (reduction/process! (nucalc-csv/line-reducible (io/reader food-file-name))
                      (take 5)
                      (map println))

  ;; parse first 5 lines to vectors

  (into []
        (take 5)
        (nucalc-csv/vector-reducible (io/reader food-file-name)))


  ;; parse first 5 lines to hash maps

  (into []
        (comp (nucalc-csv/csv-rows-to-maps)
              (take 5))
        (nucalc-csv/csv-reducible (io/reader food-file-name)))


  ;; an index definition as a function from an entity to index rows

  (defn food-to-full-text-index-rows [food]
    (for [token (map string/lower-case
                     (string/split (:description food)
                                   #" "))]
      [token (:data-type food) (:fdc-id food)]))

  (deftest test-food-to-rows
    (is (= '(["tutturosso" "branded_food" "344604"]
             ["green" "branded_food" "344604"]
             ["14.5oz." "branded_food" "344604"]
             ["nsa" "branded_food" "344604"]
             ["italian" "branded_food" "344604"]
             ["diced" "branded_food" "344604"]
             ["tomatoes" "branded_food" "344604"])
           (food-to-full-text-index-rows {:fdc-id "344604",
                                          :data-type "branded_food",
                                          :description "Tutturosso Green 14.5oz. NSA Italian Diced Tomatoes",
                                          :food-category-id "",
                                          :publication-date "2019-04-01"}))))


  ;; write a full text index as a b-tree on disk
  (def full-text-path "temp/test-full-text")

  (into [] (take 10) (nucalc-csv/hashmap-reducible-from-csv (io/reader food-file-name)))



  (do (recreate-directory full-text-path)
      (def run?-atom (write-batches-to-btree-collection! #_(eduction (take 30000)
                                                                     (nucalc-csv/hashmap-reducible-from-csv (io/reader food-file-name)))
                                                         (nucalc-csv/hashmap-reducible-from-csv (io/reader food-file-name))
                                                         50000
                                                         390294
                                                         food-to-full-text-index-rows
                                                         40000
                                                         full-text-path)))

  (def run?-atom (create-btree "temp/food-token-data-type-id"
                               (fn [food]
                                 (for [token (db-common/tokenize (:description food))]
                                   [token
                                    (:data-type food)
                                    (:fdc-id food)]))
                               [:token :data-type :fdc-id]
                               (serialization/file-reducible non-branded-foods-file-name)))

  (into [] (eduction (take 10) (serialization/file-reducible non-branded-foods-file-name)))
  (into [] (take 10) (btree-reducible "temp/non-branded-foods-full-text" []))

  (let [directory-path "temp/non-branded-foods-full-text"]
    (do (recreate-directory directory-path)
        (def run?-atom (write-batches-to-btree-collection2! (serialization/file-reducible non-branded-foods-file-name)
                                                            (fn [food]
                                                              (for [token (db-common/tokenize (:description food))]
                                                                [token
                                                                 (:data-type food)
                                                                 (:fdc-id food)]))
                                                            directory-path))))

  (def run?-atom (create-projection-btree "temp/fdc-id-data-type-description"
                                          [:fdc-id :data-type :description]
                                          (serialization/file-reducible non-branded-foods-file-name)))1

  write-batches-to-btree-collection!
  (reset! run?-atom false)
  (into [] (take 10) (btree-matching-subreducible (open-btree "temp/descriptions")
                                                  [344611]))

  (into []
        (take 10)
        (btree-subreducible (open-btree "temp/descriptions")
                            [344611]))

  #_(into [] (eduction (take 10)
                       (serialization/file-reducible non-branded-foods-file-name)))

  (let [directory-path "temp/descriptions"]
    (do (recreate-directory directory-path)
        (def run?-atom (write-batches-to-btree-collection! (serialization/file-reducible non-branded-foods-file-name)
                                                           #_(eduction (take 10) (nucalc-csv/hashmap-reducible-from-csv (io/reader food-file-name)))
                                                           50000
                                                           390294
                                                           (fn [food]
                                                             [[(Integer/parseInt (:fdc-id food))
                                                               (:data-type food)
                                                               (:description food)]])
                                                           40000
                                                           directory-path))))

  (def run?-atom (create-projection-btree "temp/nutrients"
                                          [:id
                                           :name
                                           :unit-name
                                           :nutrient-nbr
                                           :rank]
                                          (nucalc-csv/hashmap-reducible-for-csv-file nutrient-file-name
                                                                                     {:value-function parse-number})))


  (let [btree (open-btree "temp/nutrients")]
    (into [] (take 1000) (btree-reducible btree [1004])))

  (into [] (take 1000) (btree-reducible "temp/nutrients" [1004]))

  (into [] (take 1000) (btree-reducible "temp/nutrients" nil))

  (into [] (take 1000) (nucalc-csv/hashmap-reducible-from-csv (io/reader nutrient-file-name)
                                                              {:value-function parse-number}))


  (def run?-atom (create-projection-btree "temp/measurements"
                                          [:id
                                           :fdc-id
                                           :nutrient-id

                                           :amount
                                           :min
                                           :max
                                           :median

                                           :data-points
                                           :derivation-id
                                           :min-year-acquired
                                           :footnote]
                                          (serialization/file-reducible non-branded-food-nutrient-file-name)
                                          #_(eduction (take 2) (serialization/file-reducible non-branded-food-nutrient-file-name))))

  (into [] (eduction (take 2) (serialization/file-reducible non-branded-food-nutrient-file-name)))



  (into [] (take 10) (btree-reducible "temp/measurements" [9070977]))

  #_(let [directory-path "temp/measurements"]
      (do (recreate-directory directory-path)
          (def run?-atom (write-batches-to-btree-collection! (serialization/file-reducible non-branded-food-nutrient-file-name)
                                                             #_(eduction (take 10) (serialization/file-reducible non-branded-food-nutrient-file-name))
                                                             500000
                                                             1304201
                                                             (fn [food-nutrient]
                                                               [((juxt :id
                                                                       :fdc_id
                                                                       :nutrient_id

                                                                       :amount
                                                                       :min
                                                                       :max
                                                                       :median

                                                                       :data_points
                                                                       :derivation_id
                                                                       :min_year_acquired
                                                                       :footnote)
                                                                 food-nutrient)])
                                                             40000
                                                             directory-path))))

  (def run?-atom (create-projection-btree "temp/food-nutrient-amount"
                                          [:fdc-id
                                           :nutrient-id
                                           :amount
                                           :id]
                                          (serialization/file-reducible non-branded-food-nutrient-file-name)
                                          #_(eduction (take 2) (serialization/file-reducible non-branded-food-nutrient-file-name))))

  (into [] (take 10) (btree-reducible "temp/food-nutrient-amount" []))

  (let [directory-path "temp/food-to-measurements"]
    (do (recreate-directory directory-path)
        (def run?-atom (write-batches-to-btree-collection! (serialization/file-reducible non-branded-food-nutrient-file-name)
                                                           #_(eduction (take 10) (serialization/file-reducible non-branded-food-nutrient-file-name))
                                                           500000
                                                           1304201
                                                           (fn [food-nutrient]
                                                             [((juxt :fdc_id
                                                                     :nutrient_id
                                                                     :amount
                                                                     :id)
                                                               food-nutrient)])
                                                           40000
                                                           directory-path))))

  (let [directory-path "temp/measurements-by-nutrient"]
    (do (recreate-directory directory-path)
        (def run?-atom (write-batches-to-btree-collection! (serialization/file-reducible non-branded-food-nutrient-file-name)
                                                           #_(eduction (take 10) (serialization/file-reducible non-branded-food-nutrient-file-name))
                                                           500000
                                                           1304201
                                                           (fn [food-nutrient]
                                                             [((juxt :nutrient_id
                                                                     :amount
                                                                     :fdc_id
                                                                     :id)
                                                               food-nutrient)])
                                                           40000
                                                           directory-path))))

  (def run?-atom (create-full-text-btree "temp/nutrients-by-token"
                                         [:name]
                                         [:id
                                          :name
                                          :unit-name
                                          :nutrient-nbr
                                          :rank]
                                         (nucalc-csv/hashmap-reducible-for-csv-file nutrient-file-name
                                                                                    {:value-function parse-number})))

  (defn btree-first-matching [btree pattern]
    (reduction/first-from-reducible (btree-matching-subreducible btree pattern)))

  (into [] (take 10) (btree-subreducible (open-btree "temp/nutrients-by-token")
                                         ["carb"]))

  (btree-first-matching (open-btree "temp/nutrients-by-token")
                        ["carbohydrate"])


  (def run?-atom (create-projection-btree "temp/data-types"
                                          [:data-type]
                                          (serialization/file-reducible non-branded-foods-file-name)))

  (into [] (take 10) (btree-subreducible (open-btree "temp/data-types")
                                         []))


  ;; full text
  ;; Progress:                                 100.0% (390292/390294)
  ;; Processing time of the latest batch:      0:0:4.1
  ;; Processed values per second:              57
  ;; Total time passed:                        0:22:10.0
  ;; Remaining time based on the latest batch: 0:0:0.0
  ;; Remaining time based on total progress:   0:0:0.0


  (reset! run?-atom false)

  (time (into []
              (take 10)
              (sorted-reducible/subreducible (btree-collection/create-on-disk #_full-text-path
                                                                              #_"temp/descriptions"
                                                                              #_"temp/measurements"
                                                                              #_"temp/food-to-measurements"
                                                                              "temp/non-branded-foods-full-text")
                                             #_[170393])))

  (reduce reduction/count-inputs
          (sorted-reducible/subreducible (btree-collection/create-on-disk full-text-path)))

  (defn row-reducible [btree-path & pattern]
    (eduction (take-while (fn [row]
                            (= (take (count pattern)
                                     row)
                               pattern)))
              (sorted-reducible/subreducible (btree-collection/create-on-disk btree-path)
                                             pattern)))

  (into []
        (take 30)
        (row-reducible "temp/food-to-measurements"
                       167512))

  (let [query-word "carrots,"]
    (time (into []
                (comp (take-while (fn [[word]]
                                    (.startsWith word query-word)))

                      (remove (fn [[_word data-type]]
                                (= "market_acquisition" data-type)))
                      (take 100))
                (sorted-reducible/subreducible (btree-collection/create-on-disk "temp/non-branded-foods-full-text")
                                               [query-word]))))

  (btree/collect-storage-garbage (btree-collection/btree (btree-collection/create-on-disk full-text-path)))

  (def foods-path "temp/foods")
  (recreate-directory foods-path)
  (def foods-run?-atom (write-batches-to-btree-collection! (nucalc-csv/hashmap-reducible-from-csv (io/reader food-file-name))
                                                           50000
                                                           390294
                                                           food-to-row
                                                           5000
                                                           foods-path))

  ;; temp/foods
  ;; Progress:                                 100.0% (390292/390294)
  ;; Processing time of the latest batch:      0:0:2.3
  ;; Processed values per second:              993
  ;; Total time passed:                        0:2:41.3
  ;; Remaining time based on the latest batch: 0:0:0.0
  ;; Remaining time based on total progress:   0:0:0.0

  (reset! foods-run?-atom false)

  (time (into []
              (take 10)
              (sorted-reducible/subreducible (btree-collection/create-on-disk foods-path)
                                             ["170379"])))




  (def data-types-path "temp/data-types")
  (recreate-directory data-types-path)
  (def data-types-run?-atom (write-batches-to-btree-collection! (nucalc-csv/hashmap-reducible-from-csv (io/reader food-file-name))
                                                                50000
                                                                390294
                                                                (fn [food]
                                                                  [(:data-type food)])
                                                                5000
                                                                data-types-path))


  (time (into []
              (sorted-reducible/subreducible (btree-collection/create-on-disk data-types-path))))




  (btree/roots-2 (btree-collection/btree (btree-collection/create-on-disk full-text-path
                                                                          {:node-size 10000})))

  ;; read 50 first foods that contain "tomatoes" in the description







  ;; progress reporting transducer

  (transduce (reduction/report-progress-every-n-seconds 1
                                                        ;;#(prn %)
                                                        (reduction/handle-batch-ending-by-printing-report "\nTest"
                                                                                                          15))
             (completing (fn [_result _value]
                           (Thread/sleep (rand 300))
                           nil))
             nil
             (range 15))
















  (let [btree-collection (btree-collection/create-on-disk foods-path
                                                          {:node-size 1000})]
    (write-batch-to-btree-collection! (nucalc-csv/hashmap-reducible-from-csv (io/reader food-file-name))
                                      30
                                      (fn [{:keys [fdc-id data-type food-category-id description]}]
                                        [[fdc-id data-type food-category-id description]])
                                      btree-collection))


  (let [btree-collection (btree-collection/create-on-disk full-text-path
                                                          {:node-size 1000})]

    (reduction/process! (nucalc-csv/hashmap-reducible-from-csv (io/reader food-file-name))
                        (comp (drop 10)
                              (take 10))
                        (map (fn [food]
                               (run! (partial mutable-collection/add! btree-collection)
                                     (for [token (string/split (or (:description food)
                                                                   "")
                                                               #" ")]
                                       [(string/lower-case token) (:fdc-id food)])))))

    (btree-collection/locking-apply-to-btree! btree-collection
                                              btree/store-root-2 {:last-row-number 19}))

  (let [btree-collection (btree-collection/create-on-disk full-text-path
                                                          {:node-size 1000})]

    (latest-root-metadata btree-collection)

    #_(into []
            (take 4)
            (sorted-reducible/subreducible btree-collection ["tomatoes"])))

  (let [btree-collection (btree-collection/create-on-disk foods-path
                                                          {:node-size 1000})]

    (into []
          (take 2)
          (sorted-reducible/subreducible btree-collection)))

  (let [btree-collection (btree-collection/create-on-disk foods-path
                                                          {:node-size 1000})]

    (reduce reduction/first-value
            (sorted-reducible/subreducible btree-collection ["344606"])))


  (into []
        (comp (take 10)
              (map (partial make-id-keys food-key-specification))
              (map map-to-transaction/map-to-statements))
        (nucalc-csv/hashmap-reducible-from-csv (io/reader food-file-name)))


  (def count-atom (atom 0))
  (def logging-future (future (with-open [transaction-log (multifile-transaction-log/create transaction-log-path)]
                                (reduction/process! (nucalc-csv/hashmap-reducible-from-csv (io/reader food-file-name))
                                                    (map (fn [hash-map]
                                                           (update! count-atom inc)
                                                           (transaction-log/add! transaction-log hash-map)))))))

  (with-open [transaction-log (multifile-transaction-log/create transaction-log-path)]
    (reduction/process! (nucalc-csv/hashmap-reducible-from-csv (io/reader food-file-name))
                        (take 10)
                        (map (partial make-id-keys food-key-specification))
                        (map map-to-transaction/map-to-statements)
                        (map (fn [statements]
                               (transaction-log/add! transaction-log statements)))))

  (with-open [transaction-log (multifile-transaction-log/create transaction-log-path)]
    (into [] (transaction-log/subreducible transaction-log 0)))

  (with-open [transaction-log (multifile-transaction-log/create transaction-log-path)]
    (let [btree-collection (btree-collection/create-on-disk foods-path
                                                            {:node-size 1000})]

      (reduction/process! (transaction-log/subreducible transaction-log 0)
                          (map (fn [statements]
                                 (run! (partial mutable-collection/add! btree-collection)
                                       (remove nil? (for [[o e a v] statements]
                                                      (when (= :description a)
                                                        (for [token (string/split v #" ")]
                                                          [a (string/lower-case token) e]))))))))

      (into [] (sorted-reducible/subreducible btree-collection #_["food-344607"]))))


  (multifile-transaction-log/create transaction-log-path)

  (def db-atom (atom (common/deref (create-db "/Users/jukka/google-drive/src/nucalc/temp/food_nutrient"
                                              #_db-path food-nutrient-index-definitions))))

  (keys (:indexes @db-atom))

  (common/first-unindexed-transaction-number @db-atom)

  (type db-atom)

  (swap! db-atom common/update-indexes)
  (do (swap! db-atom clean-up-after-indexing)
      nil)

  (into []
        (take 10)
        (db-common/datoms-from @db-atom
                               :eav
                               ;; :text
                               ;; :data-type
                               ;; :food-nutrient-amount
                               ;; :nutrient-amount-food
                               ;; :datatype-nutrient-amount-food

                               []))

  (transaction-log/last-transaction-number (:transaction-log @db-atom))

  (:last-transaction-number @db-atom)
  (reset! running?-atom false)
  (reset! running?-atom true)
  (def my-future (future (load-data-to-db running?-atom)
                         (println "done")))



  (:val @my-future)

  (future-cancel my-future)

  (sample-indexes db-atom)

  ;; non branded foods


  (def writing-future (future (write-csv-rows-to-data-file food-file-name
                                                           non-branded-foods-file-name
                                                           (filter (fn [food]
                                                                     (not (= "branded_food"
                                                                             (:data-type food))))))))

  (def non-branded-food-id-set (into #{}
                                     (comp #_(take 3000)
                                           (map :fdc-id))
                                     (serialization/file-reducible non-branded-foods-file-name)))

  (count non-branded-food-id-set)
  (take 10 non-branded-food-id-set)

  ;; non branded food-nutrients


  (def writing-future (future (write-csv-rows-to-data-file food-nutrient-file-name
                                                           non-branded-food-nutrient-file-name
                                                           (filter (fn [food-nutrient]
                                                                     (contains? non-branded-food-id-set
                                                                                (:fdc-id food-nutrient)))))))

  (future-cancel writing-future)

  (transduce (comp (drop (inc first-input-number))
                   (reduction/report-progress-every-n-seconds 5
                                                              (reduction/handle-batch-ending-by-printing-report (str "\n" btree-path)
                                                                                                                total-count))
                   (reduction/run-every-nth batch-size finish-batch)
                   (reduction/run-while-true run?-atom)
                   (map (fn [entity]
                          (run! (partial mutable-collection/add! btree-collection)
                                (value-to-rows entity))
                          (swap! input-count-atom inc)
                          entity)))
             reduction/count-inputs
             reducible)

  (serialization/transduce-file non-branded-food-nutrient-file-name
                                :initial-value 0
                                :reducer (completing (fn [count item_]
                                                       (inc count))))

  (into []
        (comp (filter (fn [entry]
                        (some? (:median entry))))
              (take 10))
        (serialization/file-reducible  non-branded-food-nutrient-file-name
                                       #_non-branded-food-nutrient-sample-3000-file-name))

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

  (storage/get-edn-from-storage! (directory-storage/create "temp/food_nutrient/eav")
                                 "roots")

  (storage/get-edn-from-storage! (directory-storage/create "temp/test-full-text")
                                 "roots")

  (storage/get-edn-from-storage! (directory-storage/create "temp/test-full-text")
                                 "7C1C69D7B9BBBF4CD148FAD8E9E032AAAC463D1959B3D5A4961CF92326DA7B68")

  (def storage-key "C86E2C9CB6BDDF765E6022299F8F59EE6EDDC35F8066CC45E055066C6254A141")

  (node-serialization/deserialize-metadata (storage/stream-from-storage! (directory-storage/create "temp/test-full-text")
                                                                         storage-key))

  (time (into [] (take 1) (leaf-node-serialization/reducible ["carrots"]
                                                             :forwards
                                                             (:values (node-serialization/deserialize (storage/stream-from-storage! (directory-storage/create "temp/test-full-text")
                                                                                                                                    storage-key))))))

  (time (into [] (take 100) (leaf-node-serialization/reducible ::comparator/min
                                                               :forwards
                                                               (:values (node-serialization/deserialize (storage/stream-from-storage! (directory-storage/create "temp/test-full-text")
                                                                                                                                      storage-key))))))

  (node-serialization/deserialize (storage/stream-from-storage! (directory-storage/create "temp/test-full-text")
                                                                "C66F1ABDDD19D3A716640C8F4EF1E98FFCBCF3CA8E6B748448076A2A85448ABB"))



  (load-btree-2 (:storage-key (first (storage/get-edn-from-storage! (directory-storage/create "temp/test-full-text")
                                                                    "roots")))
                (directory-storage/create "temp/test-full-text"))



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
                        (map (partial make-id-keys food-nutrient-key-specification))
                        (map (partial add-type :nutrient-amount-food))))
    (swap! db-atom
           db-common/transact
           #{["food-nutrient-3639112" :amount :set 2]})

    )



  (-> @db-atom
      :indexes :food-nutrient-amount)

  (-> (atom (create-in-memory-db food-nutrient-index-definitions))
      (transact-with-transducer (map (partial make-id-keys food-nutrient-key-specification))
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



  (keys (entity "food-nutrient-9073771"))
  (into [] (common/matching-propositions (db-common/deref @db-atom)
                                         :eav
                                         ["food-nutrient-9070976"]))

  (into [] (take 100) (query/reducible-for-pattern (:collection (common/index (db-common/deref @db-atom) :eav))
                                                   ["food-nutrient-9070976"]))

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
    (transduce (comp (take 10)
                     (map last))
               conj
               (db-common/propositions-reducible  (:collection (common/index (common/deref @db-atom)
                                                                             :data-type-text))
                                                  [data-type query]
                                                  nil)))




  (defn query-foods-2 [query]
    (eduction (map last)
              (db-common/propositions-reducible (:collection (common/index (common/deref @db-atom)
                                                                           :text))
                                                [query]
                                                nil)))

  (count (into #{}
               (comp (take-while (fn [id]
                                   (.startsWith id "food")))
                     #_(take 10))
               (eduction (map first)
                         (db-common/propositions-reducible (:collection (common/index (common/deref @db-atom)
                                                                                      :eav))
                                                           ["food"]
                                                           nil))))

  (map :dali/id #_:description (map entity (query-foods "sample_food" "bacon")))

  (into [] (comp (map entity)
                 #_(filter (fn [food]
                             (= "foundation_food" (:data-type food))))
                 #_(remove (fn [food]
                             (or (= "sr_legacy_food" (:data-type food))
                                 (= "sub_sample_food" (:data-type food)))))
                 (take 10))
        (query-foods-2 "carrot"))

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
                       (map (partial make-id-keys food-key-specification))
                       (take 100))
                 :reducer conj)

  (transduce-csv food-nutrient-file-name
                 (comp (csv-rows-to-maps :parse-value (comp empty-string-to-nil
                                                            parse-number))
                       #_(remove (fn [food-nutrient]
                                   (= 0 (:amount food-nutrient))
                                   #_(= "" (:data_points food-nutrient))))
                       (take 100)
                       #_(map (partial make-id-keys food-nutrient-key-specification)))
                 :reducer conj)

  (transduce-csv nutrient-file-name
                 (comp (take 10)
                       (csv-rows-to-maps :parse-value parse-number)
                       (map (partial make-id-keys nutrient-key-specification)))
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
       (map (partial
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
                                                          (map-to-transaction/maps-to-transaction (make-id-keys food-key-specification
                                                                                                                entity))))
                                                   (map (fn [transaction]
                                                          (swap! db-atom db-common/transact transaction)))
                                                   (map (fn [_]
                                                          (swap! db-atom btree-db/store-index-roots-after-maximum-number-of-transactions 10000))))
                                             (constantly nil)
                                             non-branded-foods)))))
  @count-atom
  @db-atom

  (map-to-transaction/maps-to-transaction ((partial make-id-keys food-key-specification) {:fdc_id 321506,
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



(defn entity [id]
  (db-common/->Entity (db-common/deref @db-atom)
                      schema
                      id))

(defn run-query []
  (let [query-string ""]
    (into []
          (comp (take-while (fn [datom]
                              (.contains (first datom)
                                         query-string)))
                (map last)
                (map entity)
                #_(remove (fn [food]
                            (#{"survey_fndds_food"
                               #_"sr_legacy_food"
                               "sub_sample_food"}
                             (:data-type food))))
                (filter (fn [food]
                          (= "foundation_food" (:data-type food))))
                (take 100))
          (db-common/propositions-reducible (:collection (common/index (common/deref @db-atom)
                                                                       :text))
                                            [query-string]
                                            nil))))

(defn -main [& arguments]
  (println "moi"))

(defn dezerialize-node [directory-path node-key]
  (with-open [input-stream (storage/stream-from-storage! (directory-storage/create directory-path)
                                                         node-key)]
    (node-serialization/deserialize input-stream)))

(comment
  (def test-btree-path "temp/test-btree")
  (fs/delete-dir test-btree-path)


  (reduce mutable-collection/add!
          (btree-collection/create-in-memory {:node-size 5})
          [1 2 2 2 3])

  (reduce mutable-collection/add!
          (btree-collection/create-on-disk test-btree-path
                                           {:node-size 5})
          (range 10))

  (btree-collection/btree (btree-collection/create-on-disk test-btree-path
                                                           {:node-size 5}))

  (btree-collection/store-root! (reduce mutable-collection/add!
                                        (btree-collection/create-on-disk test-btree-path
                                                                         {:node-size 4})
                                        (range 10)))

  (into [] (sorted-reducible/subreducible (btree-collection/create-on-disk test-btree-path
                                                                           {:node-size 3})
                                          3
                                          :backwards))

  (btree-collection/btree (btree-collection/create-on-disk test-btree-path
                                                           {:node-size 3}))

  (-> (btree-collection/btree (btree-collection/create-on-disk test-btree-path
                                                               {:node-size 3}))
      (btree/load-node-3 [:root])
      (btree/load-node-3 [:root :children 1])
      (btree/load-node-3 [:root :children 2])
      (btree/load-node-3 [:root :children ::comparator/max]))


  (storage/get-edn-from-storage! (directory-storage/create test-btree-path)
                                 "roots")

  (dezerialize-node test-btree-path "00DFB6802F5EA99212DFF6762BD4C9EFC327BBBB4040AEF789F3785CB8297668")
  (dezerialize-node test-btree-path "E04F9045E3E4E378808CA0785A23CB3C78EF6E974EA7A78B86355287BB9565E4")
  ) ;; TODO: remove-me



(comment
  (test-query)
  (map #(.startsWith % "xy") [ "x"])

  (set! *warn-on-reflection* false)

  (into [] (take 10) (btree-subreducible (open-btree "temp/data-types")
                                         []))

  (into [] (take 10) (btree-subreducible (open-btree "temp/nutrients")
                                         []))

  #_(time (let [nutrient-id 1005]
            (println (:name (btree-first-matching nutrient-id-name-unit-name-nutrient-nbr-rank
                                                  [1005])))
            (reduction/process! (btree-matching-subreducible food-token-data-type-id-btree ["bean"])
                                (remove (fn [food]
                                          (= "sample_food"
                                             (:data-type food))))
                                (take 20)
                                (map (fn [food]
                                       (println (:amount (btree-first-matching food-nutrient-amount-btree
                                                                               [(:fdc-id food)]))
                                                (:data-type food)
                                                (:description (btree-first-matching fdc-id-data-type-description
                                                                                    [(:fdc-id food)]))))))
            #_(map #(dissoc % :storage-key) (btree/loaded-node-sizes (btree-collection/btree food-nutrient-amount-btree)))))

  ) ;; TODO: remove-me




















(defn test-query []
  (let [food-nutrient-amount-btree (open-btree "temp/food-nutrient-amount")
        food-token-data-type-id-btree (open-btree "temp/food-token-data-type-id")
        fdc-id-data-type-description (open-btree "temp/fdc-id-data-type-description")]
    (into []
          (take 100)
          (query/reducible-query {:?food 169885}
                                 [food-token-data-type-id-btree
                                  ["kidney" :?data-type :?food]
                                  ["beans" :?data-type :?food]]
                                 [food-nutrient-amount-btree
                                  [:?food 1005 :?amount]]
                                 [fdc-id-data-type-description
                                  [:?food :* :?description]]))))
