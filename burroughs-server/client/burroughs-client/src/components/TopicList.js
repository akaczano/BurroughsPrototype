import React from 'react';
import { connect } from 'react-redux';
import { getTopics , getSchema } from '../actions/basicActions';

class TopicList extends React.Component {
    componentDidMount() {
        setInterval(this.props.getTopics, 500);        
    }
    render() {
        return (
            <div className="topic-list">
                <div style={{ height: '40vh', overflowY: 'auto' }}>
                    <span style={{ marginLeft: '18px', fontSize: '14px' }}>
                        <strong>Topics</strong>
                    </span>
                    {this.props.topics.map(topic =>
                        <p 
                            key={topic.name} 
                            style={{ margin: '0px', marginLeft: '20px' }}
                            onClick={() => this.props.getSchema(topic.name)}
                        >
                            {topic.name}
                        </p>
                    )}
                </div>
            </div>
        );
    }
}

const mapStateToProps = state => {
    return {
        topics: state.core.topics
    };
};

export default connect(mapStateToProps, { getTopics, getSchema })(TopicList);