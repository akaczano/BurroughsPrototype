import { createStore, applyMiddleware } from 'redux';
import thunk from 'redux-thunk';
import rootReducer from './reducers';
import { composeWithDevTools } from 'redux-devtools-extension';

const initialState = {};

const middleWare = [thunk];
const composeEnhancers = composeWithDevTools({});
const store = createStore(
    rootReducer,    
    initialState,
    composeEnhancers(
        applyMiddleware(...middleWare)
    )
);
export default store;
