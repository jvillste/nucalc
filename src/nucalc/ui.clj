(ns nucalc.ui
  (:require [flow-gl.gui.animation :as animation]
            [flow-gl.gui.visuals :as visuals]
            [fungl.application :as application]
            [fungl.layouts :as layouts]
            [flow-gl.graphics.buffered-image :as buffered-image]
            [flow-gl.graphics.buffered-image :as buffered-image]
            [fungl.component.text-area :as text-area]
            [fungl.dependable-atom :as dependable-atom]
            [fungl.util :as util]
            [flow-gl.gui.keyboard :as keyboard]
            [fungl.cache :as cache]
            [clojure.test :refer :all]
            [flow-gl.graphics.font :as font]
            [nucalc.data :as data]
            [clojure.string :as string]
            [nucalc.util :as nucalc-util]
            [clojure.java.io :as io]))

;; (def font (font/create "LiberationSans-Regular.ttf"
;;                        ;;20
;;                        38))

(def font (font/create (.getPath (io/resource "LiberationSans-Regular.ttf")) 15))

(def indexes (data/open-indexes))

(defn text [string]
  (visuals/text-area string
                     [0 0 0 255]
                     font))

(defn text-area [id string & {:as options}]
  (apply text-area/text-area-2 id
         (flatten (seq (merge {:text (str string)
                               :style {:font font
                                       :color [0 0 0 255]}}
                              options)))))

(defn bare-text-editor [id text handle-text-change]
  (text-area/text-area-2 id
                         :style {:color [0 0 0 255]
                                 :font  font}
                         :text text
                         :on-text-change handle-text-change))

(defn box [content]
  (layouts/box 10
               (visuals/rectangle-2 :fill-color [255 255 255 255]
                                    :draw-color [200 200 200 255]
                                    :line-width 4
                                    :corner-arc-radius 30)
               content))

(defn text-editor [id text handle-text-change]
  (box (layouts/with-minimum-size 300 nil
         (bare-text-editor id text handle-text-change))))

(defn base-view []
  (let [state-atom (dependable-atom/atom {:uncompleted-query ""
                                          :query-results []})
        debounced-query-update (nucalc-util/debounced-function 500
                                                               (fn [new-query]

                                                                 (let [results (if (= "" new-query)
                                                                                 []
                                                                                 (take 10 (data/find-food-with-merge-join indexes
                                                                                                                          (string/split new-query
                                                                                                                                        #"\s"))))]
                                                                   (swap! state-atom assoc
                                                                          :query-results results))))]
    (fn []
      (animation/swap-state! animation/set-wake-up 1000)
      @animation/state-atom
      (layouts/superimpose (visuals/rectangle-2 :fill-color [255 255 255 255])
                           (layouts/with-margins 10 10 10 10
                             (layouts/vertically-2 {:margin 10}
                                                   (text-editor :editor
                                                                (:uncompleted-query @state-atom)
                                                                (fn [new-text]
                                                                  (swap! state-atom assoc :uncompleted-query new-text)
                                                                  (debounced-query-update new-text)))
                                                   (for [food (:query-results @state-atom)]
                                                     (text (str (:description food)
                                                                " "
                                                                (:data-type food)
                                                                " "
                                                                (:fdc-id food))))))))))

(defn start []
  (prn "----------------") ;; TODO: remove-me

  (application/start-window  #'base-view))


(comment
  (take 10 (data/find-food-with-merge-join indexes
                                           (string/split "raw carr"
                                                         #"\s")))
  ) ;; TODO: remove-me
