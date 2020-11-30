(ns nucalc.core
  (:require [argumentica.btree-index :as btree-index]
            [argumentica.mutable-collection :as mutable-collection]
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
            [clojure.core.reducers :as reducers]
            [argumentica.db.query :as query]
            [argumentica.node-serialization :as node-serialization]
            [argumentica.sorted-reducible :as sorted-reducible]
            [argumentica.db.multifile-transaction-log :as multifile-transaction-log])
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
                                       (do (fs/mkdirs (str path "/transaction-log"))
                                           (multifile-transaction-log/create (str path "/transaction-log")))))

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
                         (btree-db/unload-nodes 200)
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
                                      (db-common/composite-index-definition :text
                                                                            [{:attributes [:name :description]
                                                                              :value-function db-common/tokenize}])
                                      (db-common/enumeration-index-definition :data-type :data-type)
                                      (db-common/composite-index-definition :food-nutrient-amount [:food :nutrient :amount])
                                      (db-common/composite-index-definition :nutrient-amount-food [:nutrient :amount :food])
                                      (db-common/rule-index-definition :datatype-nutrient-amount-food {:head [:?data-type :?nutrient :?amount :?food :?description]
                                                                                                       :body [[:eav
                                                                                                               [:?measurement :amount :?amount]
                                                                                                               [:?measurement :nutrient :?nutrient]
                                                                                                               [:?measurement :food :?food]
                                                                                                               [:?food :data-type :?data-type]
                                                                                                               [:?food :description :?description]]]})])

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

                                                    (transact-csv db-atom
                                                                  nutrient-file-name
                                                                  (map (partial-right prepare nutrient-key-specification)))

                                                    (transact-data-file db-atom
                                                                        non-branded-foods-file-name
                                                                        #_(comp (drop 29000)
                                                                                (take-while (fn [_value] @running?-atom)))
                                                                        #_identity
                                                                        (take 1502)
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

(defonce running?-atom (atom true))

;;TODO: (load-data-to-db running?-atom) does not work

(comment
  (def db-atom (common/deref (create-db "/Users/jukka/google-drive/src/nucalc/temp/food_nutrient copy"
                                        #_db-path food-nutrient-index-definitions)))
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
                               #_ :data-type
                               ;; :food-nutrient-amount
                               #_:eav
                               #_:data-type-text
                               :datatype-nutrient-amount-food
                               #_:nutrient-amount-food
                               []
                               #_["food-nutrient"]))



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
