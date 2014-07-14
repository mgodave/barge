(ns barge.core
    (:require [om.core :as om :include-macros true]
              [om.dom :as dom :include-macros true]
              [clojure.string :as string]
              [barge.messaging :as m]))

(enable-console-print!)

(def node-state
  "the state of the system is a map of node ids to a connection state and  list of log messages received
ordered from newest to latest"
  (atom
    {:node1 {
      :uri "ws://127.0.0.1:56789/events"
      :ws nil
      :msgs []
      }
     :node2 {
      :uri "ws://127.0.0.1:56790/events"
      :ws nil
      :msgs []
      }
     :node3 {
      :uri "ws://127.0.0.1:56791/events"
      :ws nil
      :msgs []
      }
     }))


(defn toggle-connect [owner node connected]
  "connect/disconnect given node and updates state of owner accordingly"
  (swap! node-state (fn [st]
                      (if connected
                        (m/disconnect st node)
                        (m/connect st node node-state))))
  (om/set-state! owner :connected (not connected)))

(defn format-msg
 "format a message received from a Raft instance as a DOM node"
   [msg]
  (dom/tr nil
       (dom/td nil (:timestamp msg))
       (dom/td #js {:className (:type msg)}
               (case (:type msg)
                 "init"
                 "INIT"

                 "stateChange"
                 (str (:from msg) " -> " (:to msg))

                 "invalidTransition"
                 (str (:expected msg) " vs. " (:actual msg))

                 "stopping"
                 "STOP"

                 "appendEntries"
                 (str (count (:entriesList (:entries msg))) " entries, term: " (:term (:entries msg)))

                 "requestVote"
                 (str (:candidateId (:vote msg)) ", term: " (:term (:vote msg)))

                 "commit"
                 (str (:operation msg))

                 "" ))))


(defn messages-view
  "A view for list of messages, displaying them in a table with timestamp and content.

  Messages are formatted according to their type, and the type is also used as class name for the `td` cell
  each messages'content is put in"
  [cursor owner]
  (reify

    om/IRenderState
    (render-state [this {:keys [count]}]
        (dom/table #js {:className "pure-table"}
              (dom/thead nil
                (dom/tr nil
                  (dom/th nil "Timestamp")
                  (dom/th nil "Message")))

              ;; not sure why we have to use apply here? means that tbody is actually a function, but then
              ;; why can't I call it directly?
              (apply dom/tbody nil
                 (map format-msg
                     (take count cursor)))
                  ))))

(defn node-view
  "A view for a single node, eg. a server instance."
  [id]
  (fn [app owner]
    (reify
      om/IInitState
      (init-state [_]
        {:connected false})
      om/IRenderState
      (render-state [_ {:keys [connected]}]
        (dom/div #js {:id id}
          (dom/button
             #js {:onClick #(toggle-connect owner id (om/get-state owner :connected))
                  :className "pure-button pure-button-primary"}
            (if connected
              (str "Disconnect " (name id))
             (str "Connect " (name id))
              ))
          (om/build messages-view
                    (-> app id :msgs)
                    {:init-state {:count 15}})
          )))))


(defn set-root
  [id] (om/root (node-view id)
             node-state
          {:target (. js/document (getElementById (name id)))}))

(set-root :node1)
(set-root :node2)
(set-root :node3)


