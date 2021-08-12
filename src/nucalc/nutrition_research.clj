(ns nucalc.nutrition-research
  (:require [argumentica.map-to-transaction :as map-to-transaction]
            [argumentica.temporary-ids :as temporary-ids]
            [argumentica.statement-log :as statement-log]
            [argumentica.transaction-log :as transaction-log]
            [argumentica.db.common :as common]))


(def dabric-prelude-stream-id #uuid "cc656c18-ab66-4650-a971-c7f92d718455")

(defn maps-to-transaction [& maps]
  (let [statement-log (statement-log/in-memory)]
    (statement-log/write! statement-log
                          (apply map-to-transaction/maps-to-transaction maps))
    (second (first (into [] (transaction-log/subreducible (:transaction-log statement-log)
                                                   0))))))

(def ave-index-definition {:key :ave
                           :statements-to-changes (fn [_indexes _transaction-number statements]
                                                    (for [[o e a v] statements]
                                                      [o a v e]))})

(defn attribute-statements-to-ave-changes [attribute]
  (fn [_indexes _transaction-number statements]
    (for [[o e a v] (filter #(= attribute (nth % 2))
                            statements)]
      [a v e o])))

(defn ave-datoms [statements]
  (common/statements-to-datoms ave-index-definition
                               nil
                               0
                               statements))

(def dabric-prelude (maps-to-transaction {:dali/id :id/t-ident
                                          :id/t-ident "ident"}))

(comment

  (ave-datoms dabric-prelude)
  (common/statements-to-datoms)
  ) ;; TODO: remove-me


(defn resolve-ident [statements ident])

;; (def nutritin-research-schema (maps-to-transaction { [:dali/id dabric-prelude-stream-id "ident"] :id/t-ident "ident"}
;;                                                    ))


(defn stream []
  {:id (java.util.UUID/randomUUID)
   :next-entity-id 0})


;; TODO: create a ident index from transaction log. It should be nontemporal and only contain idents. Squash transactions and retain ident entity -pairs.
;; Allow refereing streams and entities with idents in entity id syntax so that ["dabric" "ident"] could be a reference to the ident attribute in the nutrition
;; research schema stream.
