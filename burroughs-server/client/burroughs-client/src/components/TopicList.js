import React from 'react';
import { connect } from 'react-redux';
import { Row } from 'react-bootstrap';
import { getTopics, getSchema } from '../actions/topicActions';

class TopicList extends React.Component {
    componentDidMount() {
        setInterval(this.props.getTopics, 500);
    }
    render() {
        return (
            <div className="topic-list" style={{ width: '100%' }}>
                <div style={{ backgroundColor: '#e6e6e6', width: '100%', padding: '5px', borderBottom: '1.5px ridge gray' }}>
                    <span style={{ marginLeft: '18px', fontSize: '14px', color: 'black' }}>
                        <strong>Topics</strong>
                    </span>
                </div>
                <div style={{ height: '34vh', overflowY: 'auto', width: '100%' }}>
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
        topics: state.topic.topics
    };
};

export default connect(mapStateToProps, { getTopics, getSchema })(TopicList);