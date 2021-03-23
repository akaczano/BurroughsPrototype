import client from '../api/defaultClient';
import {
    SET_TOPICS,
    SET_SCHEMA,
    TOPIC_DELETING,
    TOPIC_DELETED,
    DELETE_ERROR,
    CLOSE_DESCRIPTION,
    CLEANING_UP,
    CLEANED_UP
} from './actionTypes';

export const getTopics = () => (dispatch, getState) => {
    client
        .get('/command/topics')
        .then(topics => {
            if (getState().topic.topics.length !== topics.data.length) {
                dispatch(setTopics(topics.data));
                return;
            }            
            for (let i = 0; i < topics.data.length; i++) {
                if (getState().topic.topics[i].name !== topics.data[i].name) {
                    dispatch(setTopics(topics.data));
                    return;
                }
            }
        })
        .catch(err => {
            console.log(err);
            //dispatch({ type: LOAD_ERROR });
        });
};

export const getSchema = topic => dispatch => {    
    client
        .get('/command/topic', { params: { topicName: topic } })
        .then(schema => {
            dispatch(setSchema({
                topic: topic,
                data: schema.data
            }));
        })
        .catch(err => {
            console.log(err);
            //dispatch({ type: LOAD_ERROR });
        });
};

export const deleteTopic = topic => async dispatch => {
    dispatch({type: TOPIC_DELETING});
    try {
        await client.post('/command/drop', null, { params: {topic}});
        dispatch({type: TOPIC_DELETED});
    }
    catch(err) {
        dispatch({
            type: DELETE_ERROR,
            payload: err.response.data
        });
    }
};

export const cleanup = () => async dispatch => {
    dispatch({type: CLEANING_UP})
    try {
        await client.post('/command/cleanup');
    }
    catch(err) {}
    dispatch({type: CLEANED_UP})
};

export const setTopics = topics => {
    return {
        type: SET_TOPICS,
        payload: topics
    };
};

export const setSchema = schema => {
    return {
        type: SET_SCHEMA,
        payload: schema
    };
};

export const closeDescription = () => {
    return {
        type: CLOSE_DESCRIPTION
    };
};