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
            [clojure.test :refer :all]))

(defn text [text & [size color]]
  (visuals/text-area (str text)
                     (or color
                         [0 0 0 255])
                     (visuals/liberation-sans-regular (or size 50))))

(defn base-view []
  (let [state-atom {}]
   (fn []
     (animation/swap-state! animation/set-wake-up 1000)
     @animation/state-atom
     (layouts/superimpose (visuals/rectangle-2 :fill-color [255 255 255 255])
                          (layouts/center (layouts/vertically-2 {:margin 10}
                                                                (text "moi")))))))

(defn start []
  (prn "----------------") ;; TODO: remove-me

  (application/start-window #'base-view))
