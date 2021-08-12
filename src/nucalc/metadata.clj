(ns nucalc.metadata
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
            [clojure.edn :as edn]
            [argumentica.origin :as origin]))

(def index-definitions [db-common/eav-index-definition
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

;; TODO: create an index set in which indexes can depend on each other and can be synced with a statement log
(comment
  (name :id/id3)

  (map-to-transaction/maps-to-transaction {:type :nucalc/tag
                                           :name "vegetable"})

  #{[:add :id/t0 :name "vegetable"]
    [:add :id/t0 :type :nucalc/tag]}

  (let [log (sorted-map-transaction-log/create)
        transaction-result (-> log
                               (transaction-log/add! (map-to-transaction/maps-to-transaction {:type :nucalc/tag
                                                                                              :name "vegetable"})))]
    {:log (into [] (transaction-log/subreducible log 0))
     :transaction-result transaction-result})

  (let [origin (origin/in-memory)]
    (origin/write! origin (map-to-transaction/maps-to-transaction {:type :nucalc/tag
                                                                   :name "vegetable"}))
    (into [] (transaction-log/subreducible (:transaction-log origin)
                                           0)))

;;  TODO: create and update index based on the transaction log

  {:log [[0 ([:add :id/l0 :name "vegetable"]
             [:add :id/l0 :type :nucalc/tag])]],
   :transaction-result
   {:temporary-id-resolution #:id{:t0 :id/l0},
    :statements
    ([:add :id/l0 :name "vegetable"] [:add :id/l0 :type :nucalc/tag])}}


  {:next-id 0
   :indexes {:eav {:statements-to-changes (fn [_indexes _transaction-number statements]
                                            (for [[o e a v] statements]
                                              [o e a v]))
                   :columns [:entity :attribute :value :transaction-number :operator]
                   :collection (btree-collection/create-in-memory)}}
   :transaction-log (sorted-map-transaction-log/create)}

  (create-in-memory-db index-definitions)


  (sort (map-to-transaction/maps-to-transaction {:type :nucalc/tag
                                                 :name "vegetable"}))

  ([:add "0" :name "vegetable"]
   [:add "0" :type :nucalc/tag])


  (sort (map-to-transaction/maps-to-transaction {:type :nucalc/food
                                                 :name #{{:nucalc/language "en"
                                                          :nucalc/string "asparagus"}
                                                         {:nucalc/language "fi"
                                                          :nucalc/string "parsa"}}}))

  (map-to-transaction/maps-to-transaction {:dali/id "new-tag"
                                           :type :nucalc/tag
                                           :name "vegetable"})

  #{[:add "new-tag" :name "vegetable"]
    [:add "new-tag" :type :nucalc/tag]}

  )
