import React from 'react';

export default class ErrorBoundary extends React.Component {
  state = { error: null };

  static getDerivedStateFromError(error) {
    return { error };
  }

  componentDidCatch(error, info) {
    console.error('Unhandled error in app:', error, info);
  }

  render() {
    if (this.state.error) {
      return (
        <div className="flex flex-col items-center justify-center h-screen gap-4 text-center px-4">
          <div className="text-xl font-semibold text-gray-800">Something went wrong loading the page.</div>
          <div className="text-sm text-gray-500 max-w-md">{this.state.error.message}</div>
          <button
            className="px-4 py-2 bg-teal-600 text-white rounded hover:bg-teal-700"
            onClick={() => window.location.reload()}
          >
            Reload
          </button>
        </div>
      );
    }
    return this.props.children;
  }
}
